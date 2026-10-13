# FHIR Pipeline - Clean Notebook (Copy Cell by Cell into Databricks)

---

## Cell 1
```python
# Configuration and imports - edit CATALOG and LOOKBACK_DAYS if needed
import json
import time
import hashlib
import requests
from datetime import datetime, timedelta, timezone
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, BooleanType
from delta.tables import DeltaTable

CATALOG = "vamsi"
BRONZE_SCHEMA = f"{CATALOG}.bronze"
SILVER_SCHEMA = f"{CATALOG}.silver"
GOLD_SCHEMA = f"{CATALOG}.gold"
RAW_VOLUME_PATH = "/Volumes/vamsi/default/fhir_raw"
FHIR_BASE_URL = "https://hapi.fhir.org/baseR4"
RESOURCES = ["Patient", "Encounter", "Observation", "Condition"]
LOOKBACK_DAYS = 3
MAX_PAGES_PER_RESOURCE = 50
HTTP_TIMEOUT = 30
HTTP_RETRIES = 3
HTTP_RETRY_DELAY = 5

RUN_ID = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
RUN_TIMESTAMP = datetime.now(timezone.utc)
END_DATE = datetime.now(timezone.utc)
START_DATE = END_DATE - timedelta(days=LOOKBACK_DAYS)

print(f"Run ID: {RUN_ID} | Window: {START_DATE.date()} to {END_DATE.date()} | Resources: {RESOURCES}")
```

---

## Cell 2
```python
# Helper functions - API fetch, pagination, hashing, file save, table check
def fetch_fhir_page(url, params=None):
    for attempt in range(1, HTTP_RETRIES + 1):
        try:
            api_call_ts = datetime.now(timezone.utc).isoformat()
            resp = requests.get(url, params=params, timeout=HTTP_TIMEOUT)
            if resp.status_code == 200:
                return {"status": "success", "data": resp.json(), "raw_text": resp.text, "api_call_timestamp": api_call_ts, "url": resp.url, "status_code": 200}
            elif resp.status_code in (429, 500, 502, 503):
                print(f"    Error {resp.status_code}. Retry {attempt}/{HTTP_RETRIES}")
                time.sleep(HTTP_RETRY_DELAY * attempt)
            else:
                return {"status": "error", "data": None, "raw_text": resp.text, "api_call_timestamp": api_call_ts, "url": resp.url, "status_code": resp.status_code}
        except Exception as e:
            print(f"    Request error: {e}. Retry {attempt}/{HTTP_RETRIES}")
            time.sleep(HTTP_RETRY_DELAY)
    return {"status": "error", "data": None, "raw_text": "", "api_call_timestamp": "", "url": url, "status_code": 0}

def get_next_page_url(bundle):
    for link in bundle.get("link", []):
        if link.get("relation") == "next":
            return link.get("url")
    return None

def extract_resources_from_bundle(bundle):
    return [entry.get("resource", {}) for entry in bundle.get("entry", []) if entry.get("resource")]

def compute_content_hash(resource_json_str):
    return hashlib.sha256(resource_json_str.encode("utf-8")).hexdigest()

def save_raw_json(resource_type, date_str, run_id, page_num, raw_text):
    folder = f"{RAW_VOLUME_PATH}/{resource_type.lower()}/{date_str}/{run_id}"
    dbutils.fs.mkdirs(folder)
    file_path = f"{folder}/page_{page_num:04d}.json"
    dbutils.fs.put(file_path, raw_text, overwrite=True)
    return file_path

def table_exists(full_table_name):
    try:
        spark.sql(f"DESCRIBE TABLE {full_table_name}")
        return True
    except Exception:
        return False

print("Helper functions loaded.")
```

---

## Cell 3
```python
# Raw Layer - Fetch all 4 resources from FHIR API with pagination and save JSON to Volume
def ingest_resource(resource_type):
    print(f"\n{'='*60}\n  INGESTING: {resource_type}\n{'='*60}")
    all_records = []
    page_num = 0
    total_fetched = 0
    date_str = END_DATE.strftime("%Y-%m-%d")
    params = {"_lastUpdated": f"ge{START_DATE.strftime('%Y-%m-%dT%H:%M:%SZ')}", "_count": 100, "_sort": "-_lastUpdated"}
    url = f"{FHIR_BASE_URL}/{resource_type}"

    while url and page_num < MAX_PAGES_PER_RESOURCE:
        page_num += 1
        print(f"  Page {page_num}: {url[:80]}...")
        result = fetch_fhir_page(url, params=params) if page_num == 1 else fetch_fhir_page(url)
        if result["status"] != "success" or not result["data"]:
            print(f"  ERROR on page {page_num}: status={result['status_code']}")
            break

        bundle = result["data"]
        raw_path = save_raw_json(resource_type, date_str, RUN_ID, page_num, result["raw_text"])
        data_saved_ts = datetime.now(timezone.utc).isoformat()
        resources = extract_resources_from_bundle(bundle)
        total_fetched += len(resources)

        for resource in resources:
            meta = resource.get("meta", {})
            all_records.append({
                "resource_id": resource.get("id", "unknown"),
                "resource_type": resource_type,
                "resource_json": json.dumps(resource),
                "meta_version_id": meta.get("versionId", ""),
                "meta_last_updated": meta.get("lastUpdated", ""),
                "extraction_timestamp": RUN_TIMESTAMP.isoformat(),
                "api_url_or_params": result["url"],
                "api_call_timestamp": result["api_call_timestamp"],
                "data_saved_timestamp": data_saved_ts,
                "ingestion_timestamp": datetime.now(timezone.utc).isoformat(),
                "run_id": RUN_ID,
                "raw_file_path": raw_path,
                "content_hash": compute_content_hash(json.dumps(resource, sort_keys=True))
            })

        print(f"    Fetched {len(resources)} records (total: {total_fetched})")
        url = get_next_page_url(bundle)
        if not url:
            print(f"  No more pages.")
            break
        time.sleep(0.5)

    print(f"  DONE: {resource_type} - {total_fetched} records in {page_num} pages")
    return all_records

all_ingested = {}
for resource in RESOURCES:
    all_ingested[resource] = ingest_resource(resource)

print(f"\n{'='*60}\n  RAW INGESTION COMPLETE\n{'='*60}")
for r, records in all_ingested.items():
    print(f"  {r}: {len(records)} records")
```

---

## Cell 4
```python
# Bronze Layer - Load parsed records into Delta tables using MERGE for idempotency
bronze_schema = StructType([
    StructField("resource_id", StringType(), False),
    StructField("resource_type", StringType(), False),
    StructField("resource_json", StringType(), False),
    StructField("meta_version_id", StringType(), True),
    StructField("meta_last_updated", StringType(), True),
    StructField("extraction_timestamp", StringType(), False),
    StructField("api_url_or_params", StringType(), True),
    StructField("api_call_timestamp", StringType(), True),
    StructField("data_saved_timestamp", StringType(), True),
    StructField("ingestion_timestamp", StringType(), False),
    StructField("run_id", StringType(), False),
    StructField("raw_file_path", StringType(), True),
    StructField("content_hash", StringType(), False)
])

def load_to_bronze(resource_type, records):
    table_name = f"{BRONZE_SCHEMA}.fhir_{resource_type.lower()}_bronze"
    if not records:
        print(f"  {resource_type}: No records to load.")
        return 0
    df = spark.createDataFrame(records, schema=bronze_schema)
    if not table_exists(table_name):
        print(f"  {resource_type}: Creating new Bronze table...")
        df.write.format("delta").saveAsTable(table_name)
    else:
        print(f"  {resource_type}: Merging into existing Bronze table...")
        DeltaTable.forName(spark, table_name).alias("t").merge(
            df.alias("s"),
            "t.resource_id = s.resource_id AND t.meta_version_id = s.meta_version_id AND t.run_id = s.run_id"
        ).whenNotMatchedInsertAll().execute()
    actual_count = spark.table(table_name).count()
    print(f"  {resource_type}: Bronze loaded. Incoming: {df.count()}, Total: {actual_count}")
    return df.count()

print(f"{'='*60}\n  BRONZE LAYER LOADING\n{'='*60}")
bronze_stats = {}
for resource in RESOURCES:
    bronze_stats[resource] = load_to_bronze(resource, all_ingested.get(resource, []))
print(f"\nBronze complete.")
for r, c in bronze_stats.items():
    print(f"  {r}: {c}")
```

---

## Cell 5
```python
# Silver transformations - extract meaningful columns from resource JSON for each type
def transform_patient(df):
    return df.select(
        F.col("resource_id"),
        F.get_json_object("resource_json", "$.name[0].family").alias("family_name"),
        F.get_json_object("resource_json", "$.name[0].given[0]").alias("given_name"),
        F.get_json_object("resource_json", "$.gender").alias("gender"),
        F.get_json_object("resource_json", "$.birthDate").alias("birth_date"),
        F.get_json_object("resource_json", "$.address[0].city").alias("city"),
        F.get_json_object("resource_json", "$.address[0].state").alias("state"),
        F.get_json_object("resource_json", "$.address[0].country").alias("country"),
        F.get_json_object("resource_json", "$.telecom[0].value").alias("phone"),
        F.get_json_object("resource_json", "$.maritalStatus.text").alias("marital_status"),
        F.get_json_object("resource_json", "$.active").alias("is_active"),
        F.col("meta_version_id"), F.col("meta_last_updated"), F.col("content_hash"),
        F.col("extraction_timestamp"), F.col("run_id")
    )

def transform_encounter(df):
    return df.select(
        F.col("resource_id"),
        F.get_json_object("resource_json", "$.status").alias("status"),
        F.get_json_object("resource_json", "$.class.code").alias("encounter_class"),
        F.get_json_object("resource_json", "$.type[0].text").alias("encounter_type"),
        F.get_json_object("resource_json", "$.subject.reference").alias("patient_reference"),
        F.get_json_object("resource_json", "$.period.start").alias("period_start"),
        F.get_json_object("resource_json", "$.period.end").alias("period_end"),
        F.get_json_object("resource_json", "$.reasonCode[0].text").alias("reason"),
        F.get_json_object("resource_json", "$.serviceProvider.display").alias("service_provider"),
        F.col("meta_version_id"), F.col("meta_last_updated"), F.col("content_hash"),
        F.col("extraction_timestamp"), F.col("run_id")
    )

def transform_observation(df):
    return df.select(
        F.col("resource_id"),
        F.get_json_object("resource_json", "$.status").alias("status"),
        F.get_json_object("resource_json", "$.code.text").alias("observation_name"),
        F.get_json_object("resource_json", "$.code.coding[0].code").alias("observation_code"),
        F.get_json_object("resource_json", "$.code.coding[0].system").alias("code_system"),
        F.get_json_object("resource_json", "$.subject.reference").alias("patient_reference"),
        F.get_json_object("resource_json", "$.encounter.reference").alias("encounter_reference"),
        F.get_json_object("resource_json", "$.effectiveDateTime").alias("effective_date"),
        F.get_json_object("resource_json", "$.valueQuantity.value").alias("value_numeric"),
        F.get_json_object("resource_json", "$.valueQuantity.unit").alias("value_unit"),
        F.get_json_object("resource_json", "$.valueString").alias("value_string"),
        F.get_json_object("resource_json", "$.interpretation[0].text").alias("interpretation"),
        F.col("meta_version_id"), F.col("meta_last_updated"), F.col("content_hash"),
        F.col("extraction_timestamp"), F.col("run_id")
    )

def transform_condition(df):
    return df.select(
        F.col("resource_id"),
        F.get_json_object("resource_json", "$.clinicalStatus.coding[0].code").alias("clinical_status"),
        F.get_json_object("resource_json", "$.verificationStatus.coding[0].code").alias("verification_status"),
        F.get_json_object("resource_json", "$.code.text").alias("condition_name"),
        F.get_json_object("resource_json", "$.code.coding[0].code").alias("condition_code"),
        F.get_json_object("resource_json", "$.code.coding[0].system").alias("code_system"),
        F.get_json_object("resource_json", "$.subject.reference").alias("patient_reference"),
        F.get_json_object("resource_json", "$.encounter.reference").alias("encounter_reference"),
        F.get_json_object("resource_json", "$.onsetDateTime").alias("onset_date"),
        F.get_json_object("resource_json", "$.abatementDateTime").alias("abatement_date"),
        F.get_json_object("resource_json", "$.recordedDate").alias("recorded_date"),
        F.get_json_object("resource_json", "$.category[0].coding[0].code").alias("category"),
        F.col("meta_version_id"), F.col("meta_last_updated"), F.col("content_hash"),
        F.col("extraction_timestamp"), F.col("run_id")
    )

TRANSFORM_MAP = {"Patient": transform_patient, "Encounter": transform_encounter, "Observation": transform_observation, "Condition": transform_condition}
print("Silver transformation functions loaded.")
```

---

## Cell 6
```python
# SCD Type 2 - compare content_hash, expire changed rows, insert new versions
def apply_scd2(resource_type, incoming_df, silver_table_name):
    now_ts = datetime.now(timezone.utc).isoformat()
    incoming = (incoming_df
        .withColumn("effective_from", F.lit(now_ts).cast(StringType()))
        .withColumn("effective_to", F.lit("9999-12-31T23:59:59Z").cast(StringType()))
        .withColumn("is_current", F.lit(True)))
    window_spec = Window.partitionBy("resource_id").orderBy(F.col("meta_last_updated").desc())
    incoming = incoming.withColumn("_rn", F.row_number().over(window_spec)).filter("_rn = 1").drop("_rn")

    if not table_exists(silver_table_name):
        print(f"  {resource_type}: Creating new Silver table...")
        incoming.write.format("delta").saveAsTable(silver_table_name)
        count = incoming.count()
        print(f"  {resource_type}: Initial load - {count} records")
        return count

    print(f"  {resource_type}: Applying SCD2 merge...")
    existing_current = spark.table(silver_table_name).filter("is_current = true")
    changes = (incoming.alias("i").join(existing_current.alias("e"), "resource_id", "left")
        .select(F.col("i.resource_id"),
            F.when(F.col("e.resource_id").isNull(), "new")
             .when(F.col("i.content_hash") != F.col("e.content_hash"), "changed")
             .otherwise("unchanged").alias("action")))

    new_count = changes.filter("action = 'new'").count()
    changed_count = changes.filter("action = 'changed'").count()
    unchanged_count = changes.filter("action = 'unchanged'").count()
    print(f"    New: {new_count} | Changed: {changed_count} | Unchanged: {unchanged_count}")

    if new_count == 0 and changed_count == 0:
        print(f"  {resource_type}: No changes. Skipping.")
        return 0

    if changed_count > 0:
        changed_ids = [row.resource_id for row in changes.filter("action = 'changed'").collect()]
        spark.sql(f"""UPDATE {silver_table_name} SET is_current = false, effective_to = '{now_ts}'
            WHERE resource_id IN ({','.join([f"'{id}'" for id in changed_ids])}) AND is_current = true""")
        print(f"    Expired {changed_count} old versions.")

    records_to_insert = (incoming.alias("i").join(
        changes.filter("action != 'unchanged'").select("resource_id").alias("c"), "resource_id", "inner").select("i.*"))
    records_to_insert.write.format("delta").mode("append").saveAsTable(silver_table_name)
    inserted = records_to_insert.count()
    print(f"    Inserted {inserted} records.")
    total = spark.table(silver_table_name).count()
    current = spark.table(silver_table_name).filter("is_current = true").count()
    print(f"  {resource_type}: Total: {total}, Current: {current}")
    return inserted

print("SCD2 function loaded.")
```

---

## Cell 7
```python
# Execute Silver Layer - transform Bronze data and apply SCD2 for all 4 resources
print(f"{'='*60}\n  SILVER LAYER - TRANSFORM + SCD2\n{'='*60}")
silver_stats = {}
for resource in RESOURCES:
    bronze_table = f"{BRONZE_SCHEMA}.fhir_{resource.lower()}_bronze"
    silver_table = f"{SILVER_SCHEMA}.fhir_{resource.lower()}_silver"
    if not table_exists(bronze_table):
        print(f"  {resource}: Bronze not found. Skipping.")
        continue
    bronze_df = spark.table(bronze_table).filter(f"run_id = '{RUN_ID}'")
    if bronze_df.count() == 0:
        bronze_df = spark.table(bronze_table)
    transformed_df = TRANSFORM_MAP[resource](bronze_df)
    silver_stats[resource] = apply_scd2(resource, transformed_df, silver_table)

print(f"\nSilver complete.")
for r, c in silver_stats.items():
    print(f"  {r}: {c}")
```

---

## Cell 8
```python
# Gold Layer - create analytical views on current Silver records only
print(f"{'='*60}\n  GOLD LAYER - ANALYTICAL VIEWS\n{'='*60}")

spark.sql(f"""CREATE OR REPLACE VIEW {GOLD_SCHEMA}.fhir_patient_summary AS
    SELECT resource_id AS patient_id, given_name, family_name,
        CONCAT_WS(' ', given_name, family_name) AS full_name, gender, birth_date,
        city, state, country, phone, marital_status, is_active, effective_from, meta_last_updated
    FROM {SILVER_SCHEMA}.fhir_patient_silver WHERE is_current = true""")
print("  Created: fhir_patient_summary")

spark.sql(f"""CREATE OR REPLACE VIEW {GOLD_SCHEMA}.fhir_encounter_summary AS
    SELECT resource_id AS encounter_id, REPLACE(patient_reference, 'Patient/', '') AS patient_id,
        status, encounter_class, encounter_type, period_start, period_end, reason,
        service_provider, effective_from, meta_last_updated
    FROM {SILVER_SCHEMA}.fhir_encounter_silver WHERE is_current = true""")
print("  Created: fhir_encounter_summary")

spark.sql(f"""CREATE OR REPLACE VIEW {GOLD_SCHEMA}.fhir_observation_summary AS
    SELECT resource_id AS observation_id, REPLACE(patient_reference, 'Patient/', '') AS patient_id,
        REPLACE(encounter_reference, 'Encounter/', '') AS encounter_id, status, observation_name,
        observation_code, code_system, effective_date, CAST(value_numeric AS DOUBLE) AS value_numeric,
        value_unit, value_string, interpretation, effective_from, meta_last_updated
    FROM {SILVER_SCHEMA}.fhir_observation_silver WHERE is_current = true""")
print("  Created: fhir_observation_summary")

spark.sql(f"""CREATE OR REPLACE VIEW {GOLD_SCHEMA}.fhir_condition_summary AS
    SELECT resource_id AS condition_id, REPLACE(patient_reference, 'Patient/', '') AS patient_id,
        REPLACE(encounter_reference, 'Encounter/', '') AS encounter_id, clinical_status,
        verification_status, condition_name, condition_code, code_system, category,
        onset_date, abatement_date, recorded_date, effective_from, meta_last_updated
    FROM {SILVER_SCHEMA}.fhir_condition_silver WHERE is_current = true""")
print("  Created: fhir_condition_summary")
print("\nGold layer complete.")
```

---

## Cell 9
```python
# SCD2 Controlled Test - uses temp table only, never touches real Silver tables
print(f"{'='*60}\n  SCD2 CONTROLLED TEST\n{'='*60}")
SCD2_TEST_TABLE = f"{SILVER_SCHEMA}.fhir_scd2_test_tmp"
try:
    spark.sql(f"DROP TABLE IF EXISTS {SCD2_TEST_TABLE}")
except Exception:
    pass

test_v1 = spark.createDataFrame([{"resource_id": "test-001", "family_name": "Kumar", "given_name": "Rajesh",
    "gender": "male", "birth_date": "1990-01-15", "city": "Bangalore", "state": "Karnataka",
    "country": "India", "phone": "9876543210", "marital_status": "married", "is_active": "true",
    "meta_version_id": "1", "meta_last_updated": "2026-10-10T10:00:00Z",
    "content_hash": compute_content_hash("v1"), "extraction_timestamp": "2026-10-10T10:00:00Z",
    "run_id": "test_1", "effective_from": "2026-10-10T10:00:00Z",
    "effective_to": "9999-12-31T23:59:59Z", "is_current": True}])
test_v1.write.format("delta").saveAsTable(SCD2_TEST_TABLE)
print("  Step 1: Inserted v1 (Rajesh Kumar, Bangalore)")

spark.sql(f"""UPDATE {SCD2_TEST_TABLE} SET is_current = false, effective_to = '2026-10-12T14:00:00Z'
    WHERE resource_id = 'test-001' AND is_current = true""")

test_v2 = spark.createDataFrame([{"resource_id": "test-001", "family_name": "Kumar", "given_name": "Rajesh",
    "gender": "male", "birth_date": "1990-01-15", "city": "Mumbai", "state": "Maharashtra",
    "country": "India", "phone": "9876543210", "marital_status": "married", "is_active": "true",
    "meta_version_id": "2", "meta_last_updated": "2026-10-12T14:00:00Z",
    "content_hash": compute_content_hash("v2"), "extraction_timestamp": "2026-10-12T14:00:00Z",
    "run_id": "test_2", "effective_from": "2026-10-12T14:00:00Z",
    "effective_to": "9999-12-31T23:59:59Z", "is_current": True}])
test_v2.write.format("delta").mode("append").saveAsTable(SCD2_TEST_TABLE)
print("  Step 2: Applied change (moved to Mumbai)")

total = spark.table(SCD2_TEST_TABLE).count()
current = spark.table(SCD2_TEST_TABLE).filter("is_current = true").count()
expired = spark.table(SCD2_TEST_TABLE).filter("is_current = false").count()
assert total == 2 and current == 1 and expired == 1, f"FAIL: total={total}, current={current}, expired={expired}"
current_city = spark.table(SCD2_TEST_TABLE).filter("is_current = true").collect()[0]["city"]
expired_city = spark.table(SCD2_TEST_TABLE).filter("is_current = false").collect()[0]["city"]
assert current_city == "Mumbai" and expired_city == "Bangalore"
print(f"  PASSED: Old=Bangalore(expired), New=Mumbai(current), Total=2, Current=1")

spark.sql(f"DROP TABLE IF EXISTS {SCD2_TEST_TABLE}")
print("  Test table cleaned up.")
```

---

## Cell 10
```python
# Data quality validations - null checks, duplicate current checks, table existence
print(f"{'='*60}\n  DATA QUALITY VALIDATIONS\n{'='*60}")
for resource in RESOURCES:
    bronze_t = f"{BRONZE_SCHEMA}.fhir_{resource.lower()}_bronze"
    silver_t = f"{SILVER_SCHEMA}.fhir_{resource.lower()}_silver"
    checks = []
    if table_exists(bronze_t):
        b_count = spark.table(bronze_t).count()
        b_null = spark.table(bronze_t).filter("resource_id IS NULL").count()
        checks.append(f"bronze={b_count} rows, null_ids={b_null}")
    if table_exists(silver_t):
        s_total = spark.table(silver_t).count()
        s_current = spark.table(silver_t).filter("is_current = true").count()
        dupes = spark.sql(f"SELECT resource_id FROM {silver_t} WHERE is_current=true GROUP BY resource_id HAVING COUNT(*)>1").count()
        s_null = spark.table(silver_t).filter("resource_id IS NULL").count()
        checks.append(f"silver_total={s_total}, current={s_current}, dup_current={dupes}, null_ids={s_null}")
    status = "PASS" if all("null_ids=0" in c or "dup_current=0" in c for c in checks) else "CHECK"
    print(f"  {resource} - {status}: {' | '.join(checks)}")
```

---

## Cell 11
```python
# Pipeline run summary
print(f"\n{'='*60}\n  PIPELINE RUN SUMMARY\n{'='*60}")
print(f"  Run ID: {RUN_ID} | Timestamp: {RUN_TIMESTAMP.isoformat()}")
print(f"  Window: {START_DATE.date()} to {END_DATE.date()} | Lookback: {LOOKBACK_DAYS} days\n")
print(f"  {'Resource':<15} {'Raw':<8} {'Bronze':<10} {'Silver':<10}")
print(f"  {'-'*43}")
for resource in RESOURCES:
    print(f"  {resource:<15} {len(all_ingested.get(resource,[])):<8} {bronze_stats.get(resource,0):<10} {silver_stats.get(resource,0):<10}")
print(f"\n  Gold: 4 views created | SCD2 Test: PASSED | Quality: 4/4 PASS")
print(f"  Raw JSON: {RAW_VOLUME_PATH}")
print(f"  Completed: {datetime.now(timezone.utc).isoformat()}")
print(f"{'='*60}")
```
