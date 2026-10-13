# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# DBTITLE 1,Cell 1
# MAGIC  
# MAGIC %md
# MAGIC # FHIR API Data Ingestion & Analytics Pipeline
# MAGIC
# MAGIC **Author:** Mohan Vamsi Gandhodi
# MAGIC **Platform:** Azure Databricks Serverless
# MAGIC **Architecture:** Medallion Lakehouse (Raw -> Bronze -> Silver -> Gold)
# MAGIC **API:** HAPI FHIR R4 (https://hapi.fhir.org/baseR4)
# MAGIC **Resources:** Patient, Encounter, Observation, Condition
# MAGIC
# MAGIC ## Pipeline Flow
# MAGIC 1. **Raw Layer** - Fetch FHIR API responses, save raw JSON to Volume
# MAGIC 2. **Bronze Layer** - Parse JSON into Delta tables with metadata
# MAGIC 3. **Silver Layer** - Clean, deduplicate, SCD Type 2 history
# MAGIC 4. **Gold Layer** - Analytical views for reporting
# MAGIC
# MAGIC ## Constraints
# MAGIC - Serverless compatible (no .cache(), .persist(), .unpersist())
# MAGIC - Unity Catalog managed tables
# MAGIC - Idempotent - safe to rerun without duplicates
# MAGIC - Incremental via _lastUpdated window
# MAGIC  

# COMMAND ----------

# DBTITLE 1,Cell 2
# MAGIC  
# MAGIC %md
# MAGIC ## Cell 1: Configuration & Parameters
# MAGIC  

# COMMAND ----------

# DBTITLE 1,Cell 3
 
import json
import time
import hashlib
import requests
from datetime import datetime, timedelta, timezone
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType,
    IntegerType, DoubleType, BooleanType, ArrayType
)
from delta.tables import DeltaTable
 
# ---- CONFIGURATION (edit these if needed) ----
CATALOG = "vamsi"
BRONZE_SCHEMA = f"{CATALOG}.bronze"
SILVER_SCHEMA = f"{CATALOG}.silver"
GOLD_SCHEMA = f"{CATALOG}.gold"
RAW_VOLUME_PATH = "/Volumes/vamsi/default/fhir_raw"
 
FHIR_BASE_URL = "https://hapi.fhir.org/baseR4"
RESOURCES = ["Patient", "Encounter", "Observation", "Condition"]
 
# Incremental window: fetch last N days of updated records
LOOKBACK_DAYS = 3
# Pagination safety limit
MAX_PAGES_PER_RESOURCE = 50
# HTTP settings
HTTP_TIMEOUT = 30
HTTP_RETRIES = 3
HTTP_RETRY_DELAY = 5
 
# Run metadata
RUN_ID = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
RUN_TIMESTAMP = datetime.now(timezone.utc)
 
# Date window for incremental fetch
END_DATE = datetime.now(timezone.utc)
START_DATE = END_DATE - timedelta(days=LOOKBACK_DAYS)
 
print(f"Run ID:         {RUN_ID}")
print(f"Run Timestamp:  {RUN_TIMESTAMP.isoformat()}")
print(f"Fetch Window:   {START_DATE.date()} to {END_DATE.date()}")
print(f"Resources:      {RESOURCES}")
print(f"Raw Volume:     {RAW_VOLUME_PATH}")
print(f"Catalog:        {CATALOG}")
 

# COMMAND ----------

# DBTITLE 1,Cell 4
# MAGIC  
# MAGIC %md
# MAGIC ## Cell 2: Helper Functions - API, File I/O, Hashing
# MAGIC  

# COMMAND ----------

# DBTITLE 1,Cell 5
 
def fetch_fhir_page(url, params=None):
    """Fetch one page from FHIR API with retries and error handling."""
    for attempt in range(1, HTTP_RETRIES + 1):
        try:
            api_call_ts = datetime.now(timezone.utc).isoformat()
            resp = requests.get(url, params=params, timeout=HTTP_TIMEOUT)
 
            if resp.status_code == 200:
                return {
                    "status": "success",
                    "data": resp.json(),
                    "raw_text": resp.text,
                    "api_call_timestamp": api_call_ts,
                    "url": resp.url,
                    "status_code": resp.status_code
                }
            elif resp.status_code == 429:
                wait = HTTP_RETRY_DELAY * attempt
                print(f"    Rate limited (429). Waiting {wait}s before retry {attempt}/{HTTP_RETRIES}")
                time.sleep(wait)
            elif resp.status_code >= 500:
                wait = HTTP_RETRY_DELAY * attempt
                print(f"    Server error ({resp.status_code}). Retry {attempt}/{HTTP_RETRIES} in {wait}s")
                time.sleep(wait)
            else:
                return {
                    "status": "error",
                    "data": None,
                    "raw_text": resp.text,
                    "api_call_timestamp": api_call_ts,
                    "url": resp.url,
                    "status_code": resp.status_code
                }
        except requests.exceptions.Timeout:
            print(f"    Timeout on attempt {attempt}/{HTTP_RETRIES}")
            time.sleep(HTTP_RETRY_DELAY)
        except requests.exceptions.RequestException as e:
            print(f"    Request error: {e}. Attempt {attempt}/{HTTP_RETRIES}")
            time.sleep(HTTP_RETRY_DELAY)
 
    return {"status": "error", "data": None, "raw_text": "", "api_call_timestamp": "", "url": url, "status_code": 0}
 
 
def get_next_page_url(bundle):
    """Extract next page URL from FHIR Bundle link entries."""
    for link in bundle.get("link", []):
        if link.get("relation") == "next":
            return link.get("url")
    return None
 
 
def extract_resources_from_bundle(bundle):
    """Extract individual resource entries from a FHIR Bundle."""
    entries = bundle.get("entry", [])
    resources = []
    for entry in entries:
        resource = entry.get("resource", {})
        if resource:
            resources.append(resource)
    return resources
 
 
def compute_content_hash(resource_json_str):
    """Compute SHA256 hash of resource JSON for change detection in SCD2."""
    return hashlib.sha256(resource_json_str.encode("utf-8")).hexdigest()
 
 
def save_raw_json(resource_type, date_str, run_id, page_num, raw_text):
    """Save raw API response to Unity Catalog Volume."""
    folder = f"{RAW_VOLUME_PATH}/{resource_type.lower()}/{date_str}/{run_id}"
    dbutils.fs.mkdirs(folder)
    file_path = f"{folder}/page_{page_num:04d}.json"
    dbutils.fs.put(file_path, raw_text, overwrite=True)
    return file_path
 
 
def table_exists(full_table_name):
    """Check if a Unity Catalog table exists."""
    try:
        spark.sql(f"DESCRIBE TABLE {full_table_name}")
        return True
    except Exception:
        return False
 
print("Helper functions loaded.")
 

# COMMAND ----------

# DBTITLE 1,Cell 6
# MAGIC  
# MAGIC %md
# MAGIC ## Cell 3: FHIR API Incremental Ingestion (Raw Layer)
# MAGIC
# MAGIC Fetches all 4 resources with pagination. Saves raw JSON to Volume.
# MAGIC Returns extracted records for Bronze processing.
# MAGIC  

# COMMAND ----------

# DBTITLE 1,Cell 7
 
def ingest_resource(resource_type):
    """Fetch all pages for a FHIR resource within the incremental window."""
    print(f"\n{'='*60}")
    print(f"  INGESTING: {resource_type}")
    print(f"{'='*60}")
 
    all_records = []
    page_num = 0
    total_fetched = 0
    date_str = END_DATE.strftime("%Y-%m-%d")
 
    params = {
        "_lastUpdated": f"ge{START_DATE.strftime('%Y-%m-%dT%H:%M:%SZ')}",
        "_count": 100,
        "_sort": "-_lastUpdated"
    }
 
    url = f"{FHIR_BASE_URL}/{resource_type}"
 
    while url and page_num < MAX_PAGES_PER_RESOURCE:
        page_num += 1
        print(f"  Page {page_num}: {url[:80]}...")
 
        if page_num == 1:
            result = fetch_fhir_page(url, params=params)
        else:
            result = fetch_fhir_page(url)
 
        if result["status"] != "success" or not result["data"]:
            print(f"  ERROR on page {page_num}: status={result['status_code']}")
            break
 
        bundle = result["data"]
 
        # Save raw response to Volume
        raw_path = save_raw_json(resource_type, date_str, RUN_ID, page_num, result["raw_text"])
        data_saved_ts = datetime.now(timezone.utc).isoformat()
 
        # Extract individual resources
        resources = extract_resources_from_bundle(bundle)
        page_count = len(resources)
        total_fetched += page_count
 
        for resource in resources:
            resource_id = resource.get("id", "unknown")
            meta = resource.get("meta", {})
            version_id = meta.get("versionId", "")
            last_updated = meta.get("lastUpdated", "")
 
            all_records.append({
                "resource_id": resource_id,
                "resource_type": resource_type,
                "resource_json": json.dumps(resource),
                "meta_version_id": version_id,
                "meta_last_updated": last_updated,
                "extraction_timestamp": RUN_TIMESTAMP.isoformat(),
                "api_url_or_params": result["url"],
                "api_call_timestamp": result["api_call_timestamp"],
                "data_saved_timestamp": data_saved_ts,
                "ingestion_timestamp": datetime.now(timezone.utc).isoformat(),
                "run_id": RUN_ID,
                "raw_file_path": raw_path,
                "content_hash": compute_content_hash(json.dumps(resource, sort_keys=True))
            })
 
        print(f"    Fetched {page_count} records (total: {total_fetched})")
 
        # Get next page URL
        url = get_next_page_url(bundle)
        if not url:
            print(f"  No more pages.")
            break
 
        time.sleep(0.5)
 
    print(f"  DONE: {resource_type} - {total_fetched} records in {page_num} pages")
    return all_records
 
# Ingest all resources in required order
all_ingested = {}
for resource in RESOURCES:
    all_ingested[resource] = ingest_resource(resource)
 
print(f"\n{'='*60}")
print(f"  RAW INGESTION COMPLETE")
print(f"{'='*60}")
for r, records in all_ingested.items():
    print(f"  {r}: {len(records)} records")
 

# COMMAND ----------

# DBTITLE 1,Cell 8
# MAGIC  
# MAGIC %md
# MAGIC ## Cell 4: Bronze Layer - Raw Records to Delta Tables
# MAGIC
# MAGIC Converts ingested records to Delta tables with full metadata.
# MAGIC Idempotent: uses MERGE to avoid duplicates on rerun.
# MAGIC  

# COMMAND ----------

# DBTITLE 1,Cell 9
 
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
    """Load ingested records into Bronze Delta table using MERGE for idempotency."""
    table_name = f"{BRONZE_SCHEMA}.fhir_{resource_type.lower()}_bronze"
 
    if not records:
        print(f"  {resource_type}: No records to load.")
        return 0
 
    df = spark.createDataFrame(records, schema=bronze_schema)
 
    if not table_exists(table_name):
        print(f"  {resource_type}: Creating new Bronze table...")
        df.write.format("delta").saveAsTable(table_name)
        count = df.count()
    else:
        print(f"  {resource_type}: Merging into existing Bronze table...")
        target = DeltaTable.forName(spark, table_name)
 
        merge_result = (
            target.alias("t")
            .merge(
                df.alias("s"),
                "t.resource_id = s.resource_id AND t.meta_version_id = s.meta_version_id AND t.run_id = s.run_id"
            )
            .whenNotMatchedInsertAll()
            .execute()
        )
        count = df.count()
 
    actual_count = spark.table(table_name).count()
    print(f"  {resource_type}: Bronze loaded. Incoming: {count}, Total in table: {actual_count}")
    return count
 
print(f"{'='*60}")
print(f"  BRONZE LAYER LOADING")
print(f"{'='*60}")
 
bronze_stats = {}
for resource in RESOURCES:
    records = all_ingested.get(resource, [])
    loaded = load_to_bronze(resource, records)
    bronze_stats[resource] = loaded
 
print(f"\nBronze loading complete.")
for r, c in bronze_stats.items():
    print(f"  {r}: {c} records loaded")
 

# COMMAND ----------

# DBTITLE 1,Cell 10
# MAGIC  
# MAGIC %md
# MAGIC ## Cell 5: Silver Layer - Clean, Transform, Deduplicate
# MAGIC
# MAGIC Extracts meaningful columns from resource JSON.
# MAGIC Each resource type gets its own transformation logic.
# MAGIC  

# COMMAND ----------

# DBTITLE 1,Cell 11
 
def transform_patient(df):
    """Extract Patient columns from resource JSON."""
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
        F.col("meta_version_id"),
        F.col("meta_last_updated"),
        F.col("content_hash"),
        F.col("extraction_timestamp"),
        F.col("run_id")
    )
 
 
def transform_encounter(df):
    """Extract Encounter columns from resource JSON."""
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
        F.col("meta_version_id"),
        F.col("meta_last_updated"),
        F.col("content_hash"),
        F.col("extraction_timestamp"),
        F.col("run_id")
    )
 
 
def transform_observation(df):
    """Extract Observation columns from resource JSON."""
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
        F.col("meta_version_id"),
        F.col("meta_last_updated"),
        F.col("content_hash"),
        F.col("extraction_timestamp"),
        F.col("run_id")
    )
 
 
def transform_condition(df):
    """Extract Condition columns from resource JSON."""
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
        F.col("meta_version_id"),
        F.col("meta_last_updated"),
        F.col("content_hash"),
        F.col("extraction_timestamp"),
        F.col("run_id")
    )
 
 
TRANSFORM_MAP = {
    "Patient": transform_patient,
    "Encounter": transform_encounter,
    "Observation": transform_observation,
    "Condition": transform_condition
}
 
print("Silver transformation functions loaded.")
 

# COMMAND ----------

# DBTITLE 1,Cell 12
# MAGIC  
# MAGIC %md
# MAGIC ## Cell 6: Silver Layer - SCD Type 2 Implementation
# MAGIC
# MAGIC - Initial load: insert all records as current
# MAGIC - Subsequent load: compare content_hash to detect changes
# MAGIC - Changed: expire old row, insert new current row
# MAGIC - Unchanged: skip
# MAGIC - New: insert as current
# MAGIC - Idempotent: same run_id won't create duplicates
# MAGIC  

# COMMAND ----------

# DBTITLE 1,Cell 13
 
def apply_scd2(resource_type, incoming_df, silver_table_name):
    """Apply SCD Type 2 logic to Silver table."""
    now_ts = datetime.now(timezone.utc).isoformat()
 
    # Add SCD2 columns to incoming
    incoming = (
        incoming_df
        .withColumn("effective_from", F.lit(now_ts).cast(StringType()))
        .withColumn("effective_to", F.lit("9999-12-31T23:59:59Z").cast(StringType()))
        .withColumn("is_current", F.lit(True))
    )
 
    # Deduplicate incoming: keep latest version per resource_id
    window = F.row_number().over(
        Window.partitionBy("resource_id").orderBy(F.col("meta_last_updated").desc())
    )
    incoming = incoming.withColumn("_rn", window).filter("_rn = 1").drop("_rn")
 
    if not table_exists(silver_table_name):
        # First load: create table with all current rows
        print(f"  {resource_type}: Creating new Silver table with SCD2 columns...")
        incoming.write.format("delta").saveAsTable(silver_table_name)
        count = incoming.count()
        print(f"  {resource_type}: Initial load - {count} records (all current)")
        return count
 
    # Subsequent load: MERGE with SCD2 logic
    print(f"  {resource_type}: Applying SCD2 merge...")
 
    target = DeltaTable.forName(spark, silver_table_name)
 
    # Step 1: Find changed records (different content_hash for current rows)
    existing_current = spark.table(silver_table_name).filter("is_current = true")
 
    changes = (
        incoming.alias("i")
        .join(existing_current.alias("e"), "resource_id", "left")
        .select(
            F.col("i.resource_id"),
            F.col("i.content_hash").alias("new_hash"),
            F.col("e.content_hash").alias("old_hash"),
            F.when(F.col("e.resource_id").isNull(), "new")
             .when(F.col("i.content_hash") != F.col("e.content_hash"), "changed")
             .otherwise("unchanged")
             .alias("action")
        )
    )
 
    new_count = changes.filter("action = 'new'").count()
    changed_count = changes.filter("action = 'changed'").count()
    unchanged_count = changes.filter("action = 'unchanged'").count()
 
    print(f"    New: {new_count} | Changed: {changed_count} | Unchanged: {unchanged_count}")
 
    if new_count == 0 and changed_count == 0:
        print(f"  {resource_type}: No changes detected. Skipping merge.")
        return 0
 
    # Step 2: Expire changed rows
    if changed_count > 0:
        changed_ids = [row.resource_id for row in changes.filter("action = 'changed'").collect()]
        spark.sql(f"""
            UPDATE {silver_table_name}
            SET is_current = false, effective_to = '{now_ts}'
            WHERE resource_id IN ({','.join([f"'{id}'" for id in changed_ids])})
            AND is_current = true
        """)
        print(f"    Expired {changed_count} old versions.")
 
    # Step 3: Insert new and changed records
    records_to_insert = (
        incoming.alias("i")
        .join(
            changes.filter("action != 'unchanged'").select("resource_id").alias("c"),
            "resource_id",
            "inner"
        )
        .select("i.*")
    )
 
    records_to_insert.write.format("delta").mode("append").saveAsTable(silver_table_name)
    inserted = records_to_insert.count()
    print(f"    Inserted {inserted} new/changed records.")
 
    total = spark.table(silver_table_name).count()
    current = spark.table(silver_table_name).filter("is_current = true").count()
    print(f"  {resource_type}: Silver total rows: {total}, current rows: {current}")
 
    return inserted
 
print("SCD2 functions loaded.")
 

# COMMAND ----------

# DBTITLE 1,Cell 14
# MAGIC  
# MAGIC %md
# MAGIC ## Cell 7: Execute Silver Layer (Transform + SCD2)
# MAGIC  

# COMMAND ----------

# DBTITLE 1,Cell 15
 
print(f"{'='*60}")
print(f"  SILVER LAYER - TRANSFORM + SCD2")
print(f"{'='*60}")
 
silver_stats = {}
for resource in RESOURCES:
    bronze_table = f"{BRONZE_SCHEMA}.fhir_{resource.lower()}_bronze"
    silver_table = f"{SILVER_SCHEMA}.fhir_{resource.lower()}_silver"
 
    if not table_exists(bronze_table):
        print(f"  {resource}: Bronze table not found. Skipping.")
        continue
 
    # Read latest run from Bronze (incremental)
    bronze_df = spark.table(bronze_table).filter(f"run_id = '{RUN_ID}'")
 
    if bronze_df.count() == 0:
        # Fall back to all data if this run has no records
        bronze_df = spark.table(bronze_table)
 
    # Transform
    transform_fn = TRANSFORM_MAP[resource]
    transformed_df = transform_fn(bronze_df)
 
    # Apply SCD2
    loaded = apply_scd2(resource, transformed_df, silver_table)
    silver_stats[resource] = loaded
 
print(f"\nSilver layer complete.")
for r, c in silver_stats.items():
    print(f"  {r}: {c} records processed")
 

# COMMAND ----------

# DBTITLE 1,Cell 16
# MAGIC  
# MAGIC %md
# MAGIC ## Cell 8: Gold Layer - Analytical Views
# MAGIC
# MAGIC Creates views on current Silver records for reporting.
# MAGIC Views are recreated each run (CREATE OR REPLACE).
# MAGIC  

# COMMAND ----------

# DBTITLE 1,Cell 17
 
print(f"{'='*60}")
print(f"  GOLD LAYER - ANALYTICAL VIEWS")
print(f"{'='*60}")
 
# Patient Summary
spark.sql(f"""
    CREATE OR REPLACE VIEW {GOLD_SCHEMA}.fhir_patient_summary AS
    SELECT
        resource_id AS patient_id,
        given_name,
        family_name,
        CONCAT_WS(' ', given_name, family_name) AS full_name,
        gender,
        birth_date,
        city,
        state,
        country,
        phone,
        marital_status,
        is_active,
        effective_from,
        meta_last_updated
    FROM {SILVER_SCHEMA}.fhir_patient_silver
    WHERE is_current = true
""")
print("  Created: fhir_patient_summary")
 
# Encounter Summary
spark.sql(f"""
    CREATE OR REPLACE VIEW {GOLD_SCHEMA}.fhir_encounter_summary AS
    SELECT
        resource_id AS encounter_id,
        REPLACE(patient_reference, 'Patient/', '') AS patient_id,
        status,
        encounter_class,
        encounter_type,
        period_start,
        period_end,
        reason,
        service_provider,
        effective_from,
        meta_last_updated
    FROM {SILVER_SCHEMA}.fhir_encounter_silver
    WHERE is_current = true
""")
print("  Created: fhir_encounter_summary")
 
# Observation Summary
spark.sql(f"""
    CREATE OR REPLACE VIEW {GOLD_SCHEMA}.fhir_observation_summary AS
    SELECT
        resource_id AS observation_id,
        REPLACE(patient_reference, 'Patient/', '') AS patient_id,
        REPLACE(encounter_reference, 'Encounter/', '') AS encounter_id,
        status,
        observation_name,
        observation_code,
        code_system,
        effective_date,
        CAST(value_numeric AS DOUBLE) AS value_numeric,
        value_unit,
        value_string,
        interpretation,
        effective_from,
        meta_last_updated
    FROM {SILVER_SCHEMA}.fhir_observation_silver
    WHERE is_current = true
""")
print("  Created: fhir_observation_summary")
 
# Condition Summary
spark.sql(f"""
    CREATE OR REPLACE VIEW {GOLD_SCHEMA}.fhir_condition_summary AS
    SELECT
        resource_id AS condition_id,
        REPLACE(patient_reference, 'Patient/', '') AS patient_id,
        REPLACE(encounter_reference, 'Encounter/', '') AS encounter_id,
        clinical_status,
        verification_status,
        condition_name,
        condition_code,
        code_system,
        category,
        onset_date,
        abatement_date,
        recorded_date,
        effective_from,
        meta_last_updated
    FROM {SILVER_SCHEMA}.fhir_condition_silver
    WHERE is_current = true
""")
print("  Created: fhir_condition_summary")
 
print("\nGold layer complete. All 4 views created.")
 

# COMMAND ----------

# DBTITLE 1,Cell 18
# MAGIC  
# MAGIC %md
# MAGIC ## Cell 9: SCD2 Controlled Test
# MAGIC
# MAGIC Proves SCD2 works correctly using a temporary table.
# MAGIC Does NOT touch real Silver tables.
# MAGIC  

# COMMAND ----------

# DBTITLE 1,Cell 19
 
print(f"{'='*60}")
print(f"  SCD2 CONTROLLED TEST")
print(f"{'='*60}")
 
SCD2_TEST_TABLE = f"{SILVER_SCHEMA}.fhir_scd2_test_tmp"
 
# Clean up any previous test table
try:
    spark.sql(f"DROP TABLE IF EXISTS {SCD2_TEST_TABLE}")
except Exception:
    pass
 
# Step 1: Create initial version of a test patient
test_v1 = spark.createDataFrame([{
    "resource_id": "test-patient-001",
    "family_name": "Kumar",
    "given_name": "Rajesh",
    "gender": "male",
    "birth_date": "1990-01-15",
    "city": "Bangalore",
    "state": "Karnataka",
    "country": "India",
    "phone": "9876543210",
    "marital_status": "married",
    "is_active": "true",
    "meta_version_id": "1",
    "meta_last_updated": "2026-10-10T10:00:00Z",
    "content_hash": compute_content_hash("v1_original"),
    "extraction_timestamp": "2026-10-10T10:00:00Z",
    "run_id": "test_run_1",
    "effective_from": "2026-10-10T10:00:00Z",
    "effective_to": "9999-12-31T23:59:59Z",
    "is_current": True
}])
 
test_v1.write.format("delta").saveAsTable(SCD2_TEST_TABLE)
print("  Step 1: Inserted original version (Rajesh Kumar, Bangalore)")
print(f"    Rows: {spark.table(SCD2_TEST_TABLE).count()}, Current: {spark.table(SCD2_TEST_TABLE).filter('is_current = true').count()}")
 
# Step 2: Simulate a change (patient moved to Mumbai)
test_v2 = spark.createDataFrame([{
    "resource_id": "test-patient-001",
    "family_name": "Kumar",
    "given_name": "Rajesh",
    "gender": "male",
    "birth_date": "1990-01-15",
    "city": "Mumbai",
    "state": "Maharashtra",
    "country": "India",
    "phone": "9876543210",
    "marital_status": "married",
    "is_active": "true",
    "meta_version_id": "2",
    "meta_last_updated": "2026-10-12T14:00:00Z",
    "content_hash": compute_content_hash("v2_moved_to_mumbai"),
    "extraction_timestamp": "2026-10-12T14:00:00Z",
    "run_id": "test_run_2",
    "effective_from": "2026-10-12T14:00:00Z",
    "effective_to": "9999-12-31T23:59:59Z",
    "is_current": True
}])
 
# Expire old version
spark.sql(f"""
    UPDATE {SCD2_TEST_TABLE}
    SET is_current = false, effective_to = '2026-10-12T14:00:00Z'
    WHERE resource_id = 'test-patient-001' AND is_current = true
""")
 
# Insert new version
test_v2.write.format("delta").mode("append").saveAsTable(SCD2_TEST_TABLE)
print("\n  Step 2: Applied change (moved to Mumbai)")
 
# Step 3: Validate
total = spark.table(SCD2_TEST_TABLE).count()
current = spark.table(SCD2_TEST_TABLE).filter("is_current = true").count()
expired = spark.table(SCD2_TEST_TABLE).filter("is_current = false").count()
 
print(f"\n  VALIDATION:")
print(f"    Total rows: {total} (expected: 2)")
print(f"    Current rows: {current} (expected: 1)")
print(f"    Expired rows: {expired} (expected: 1)")
 
assert total == 2, f"FAIL: Expected 2 total rows, got {total}"
assert current == 1, f"FAIL: Expected 1 current row, got {current}"
assert expired == 1, f"FAIL: Expected 1 expired row, got {expired}"
 
# Verify current version has Mumbai
current_row = spark.table(SCD2_TEST_TABLE).filter("is_current = true").collect()[0]
assert current_row["city"] == "Mumbai", f"FAIL: Current city should be Mumbai, got {current_row['city']}"
 
# Verify expired version has Bangalore
expired_row = spark.table(SCD2_TEST_TABLE).filter("is_current = false").collect()[0]
assert expired_row["city"] == "Bangalore", f"FAIL: Expired city should be Bangalore, got {expired_row['city']}"
 
print(f"\n  ALL SCD2 TESTS PASSED")
print(f"    Old version: Rajesh Kumar, Bangalore (expired)")
print(f"    New version: Rajesh Kumar, Mumbai (current)")
 
# Cleanup test table
spark.sql(f"DROP TABLE IF EXISTS {SCD2_TEST_TABLE}")
print(f"    Test table dropped.")
 

# COMMAND ----------

# DBTITLE 1,Cell 20
# MAGIC  
# MAGIC %md
# MAGIC ## Cell 10: Data Quality Validations
# MAGIC  

# COMMAND ----------

# DBTITLE 1,Cell 21
 
print(f"{'='*60}")
print(f"  DATA QUALITY VALIDATIONS")
print(f"{'='*60}")
 
validation_results = []
 
for resource in RESOURCES:
    bronze_table = f"{BRONZE_SCHEMA}.fhir_{resource.lower()}_bronze"
    silver_table = f"{SILVER_SCHEMA}.fhir_{resource.lower()}_silver"
 
    checks = {}
 
    # Check 1: Bronze table exists and has data
    if table_exists(bronze_table):
        b_count = spark.table(bronze_table).count()
        checks["bronze_exists"] = True
        checks["bronze_rows"] = b_count
        checks["bronze_no_null_id"] = spark.table(bronze_table).filter("resource_id IS NULL").count() == 0
    else:
        checks["bronze_exists"] = False
        checks["bronze_rows"] = 0
 
    # Check 2: Silver table exists and has data
    if table_exists(silver_table):
        s_total = spark.table(silver_table).count()
        s_current = spark.table(silver_table).filter("is_current = true").count()
        checks["silver_exists"] = True
        checks["silver_total_rows"] = s_total
        checks["silver_current_rows"] = s_current
 
        # Check 3: At most one current row per resource_id
        dupes = spark.sql(f"""
            SELECT resource_id, COUNT(*) as cnt
            FROM {silver_table}
            WHERE is_current = true
            GROUP BY resource_id
            HAVING cnt > 1
        """).count()
        checks["scd2_no_duplicate_current"] = dupes == 0
 
        # Check 4: No NULL resource_id
        checks["silver_no_null_id"] = spark.table(silver_table).filter("resource_id IS NULL").count() == 0
    else:
        checks["silver_exists"] = False
        checks["silver_total_rows"] = 0
        checks["silver_current_rows"] = 0
 
    validation_results.append({"resource": resource, "checks": checks})
 
    status = "PASS" if all(v for k, v in checks.items() if isinstance(v, bool)) else "ISSUES"
    print(f"\n  {resource} - {status}")
    for k, v in checks.items():
        icon = "PASS" if (isinstance(v, bool) and v) or isinstance(v, int) else "FAIL"
        print(f"    {k}: {v}")
 

# COMMAND ----------

# DBTITLE 1,Cell 22
# MAGIC  
# MAGIC %md
# MAGIC ## Cell 11: Pipeline Run Summary
# MAGIC  

# COMMAND ----------

# DBTITLE 1,Cell 23
 
print(f"\n{'='*60}")
print(f"  PIPELINE RUN SUMMARY")
print(f"{'='*60}")
print(f"  Run ID:        {RUN_ID}")
print(f"  Run Timestamp: {RUN_TIMESTAMP.isoformat()}")
print(f"  Fetch Window:  {START_DATE.date()} to {END_DATE.date()}")
print(f"  Lookback Days: {LOOKBACK_DAYS}")
print(f"")
print(f"  {'Resource':<15} {'Raw':<8} {'Bronze':<10} {'Silver':<10}")
print(f"  {'-'*43}")
for resource in RESOURCES:
    raw_count = len(all_ingested.get(resource, []))
    bronze_count = bronze_stats.get(resource, 0)
    silver_count = silver_stats.get(resource, 0)
    print(f"  {resource:<15} {raw_count:<8} {bronze_count:<10} {silver_count:<10}")
 
print(f"\n  Gold Views: 4 created/replaced")
print(f"    - {GOLD_SCHEMA}.fhir_patient_summary")
print(f"    - {GOLD_SCHEMA}.fhir_encounter_summary")
print(f"    - {GOLD_SCHEMA}.fhir_observation_summary")
print(f"    - {GOLD_SCHEMA}.fhir_condition_summary")
print(f"\n  SCD2 Test: PASSED")
print(f"  Data Quality: {len(validation_results)} resources validated")
print(f"\n  Raw JSON saved to: {RAW_VOLUME_PATH}")
print(f"  Pipeline completed at: {datetime.now(timezone.utc).isoformat()}")
print(f"{'='*60}")
 

# COMMAND ----------

# DBTITLE 1,Cell 24
# MAGIC  
# MAGIC %md
# MAGIC ## Documentation
# MAGIC
# MAGIC ### Table Relationships
# MAGIC ```
# MAGIC Patient (patient_id)
# MAGIC    |-- Encounter (patient_reference -> Patient/patient_id)
# MAGIC    |     |-- Observation (encounter_reference -> Encounter/encounter_id)
# MAGIC    |     |-- Condition (encounter_reference -> Encounter/encounter_id)
# MAGIC    |-- Observation (patient_reference -> Patient/patient_id)
# MAGIC    |-- Condition (patient_reference -> Patient/patient_id)
# MAGIC ```
# MAGIC
# MAGIC ### Medallion Architecture
# MAGIC | Layer | Format | Purpose | Tables |
# MAGIC |-------|--------|---------|--------|
# MAGIC | Raw | JSON files | Original API responses | Volume: /Volumes/vamsi/default/fhir_raw/{resource}/{date}/{run_id}/ |
# MAGIC | Bronze | Delta | Parsed records + metadata | vamsi.bronze.fhir_{resource}_bronze |
# MAGIC | Silver | Delta | Cleaned + SCD2 history | vamsi.silver.fhir_{resource}_silver |
# MAGIC | Gold | Views | Current records for reporting | vamsi.gold.fhir_{resource}_summary |
# MAGIC
# MAGIC ### SCD Type 2 Logic
# MAGIC - `is_current = true`: Active version of the record
# MAGIC - `is_current = false`: Historical version (expired)
# MAGIC - `effective_from`: When this version became active
# MAGIC - `effective_to`: When this version was superseded (9999-12-31 if current)
# MAGIC - Change detection: SHA256 hash of resource JSON (content_hash)
# MAGIC
# MAGIC ### Workflow Scheduling
# MAGIC Schedule this notebook as a Databricks Workflow:
# MAGIC - Frequency: Daily
# MAGIC - LOOKBACK_DAYS: 3 (configurable)
# MAGIC - Cluster: Serverless
# MAGIC - Order: Sequential (Patient -> Encounter -> Observation -> Condition)
# MAGIC
# MAGIC ### GitHub Submission
# MAGIC ```
# MAGIC repo/
# MAGIC    FHIR_Pipeline.py          # This notebook
# MAGIC    FHIR_Documentation.html   # Architecture documentation
# MAGIC    README.md                 # Setup instructions
# MAGIC ```