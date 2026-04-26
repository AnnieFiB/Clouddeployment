# ==================================================================================
# # Batch ETL Pipeline (Cloud deployment)
# ## Case Study( On-Cloud) - GCP, GCS, Airflow, BigQuery
# ==================================================================================
# ### Libraries and Dependencies

import os
import tempfile
from pathlib import Path
import gdown
from google.cloud import storage , bigquery
from datetime import datetime
import apache_beam as beam 
import re
from apache_beam.options.pipeline_options import PipelineOptions    
import argparse

from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())  

DRIVE_FILE_ID="1GRyRW2pW3isbbwBPW7lcJrS-yeuZGPj_"
gdrive_url = f"https://drive.google.com/uc?id={DRIVE_FILE_ID}"

PROJECT_ID   = os.getenv("GCP_PROJECT_ID")
BUCKET_NAME  = os.getenv("GCS_BUCKET")   
BUCKET_NAME2  = os.getenv("GCS_BUCKET2")                  
RAW_OBJECT   = os.getenv("GCS_OBJECT", "trips/ncr_ride_bookings.csv")
OUT_PREFIX   = os.getenv("OUT_PREFIX", "warehouse")
GOOGLE_APPLICATION_CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
BIGQUERY_DATASET = os.getenv("BIGQUERY_DATASET", "uber_dataset")

if not BUCKET_NAME or not GOOGLE_APPLICATION_CREDENTIALS:
    raise SystemExit("Set GCS_BUCKET and GOOGLE_APPLICATION_CREDENTIALS in .env")

date_table_spec= f"{PROJECT_ID}:{BIGQUERY_DATASET}.dim_date"
customer_table_spec = f"{PROJECT_ID}:{BIGQUERY_DATASET}.dim_customer"
vehicle_table_spec = f"{PROJECT_ID}:{BIGQUERY_DATASET}.dim_vehicle"
location_table_spec = f"{PROJECT_ID}:{BIGQUERY_DATASET}.dim_location"
payment_table_spec = f"{PROJECT_ID}:{BIGQUERY_DATASET}.dim_payment"
reason_table_spec = f"{PROJECT_ID}:{BIGQUERY_DATASET}.dim_reason"
fact_table_spec = f"{PROJECT_ID}:{BIGQUERY_DATASET}.fact_uber_trips" 

#Initialise BigQuery client
bq_client = bigquery.Client()
dataset_id = f"{PROJECT_ID}:{BIGQUERY_DATASET}"

try:
    bq_client.get_dataset(dataset_id)
except Exception:
    dataset_ref = bigquery.DatasetReference(PROJECT_ID, BIGQUERY_DATASET)
    dataset = bigquery.Dataset(dataset_ref)
    dataset.location = "europe-west2"
    dataset.description = "Uber dataset for analysis"
    bq_client.create_dataset(dataset, exists_ok=True)


# ====================
# ### Extraction Layer
# ====================
# - Download the uber data from Google Drive to Google Cloud Storage bucket (uber_raw)

def extract_and_upload():
    """
    Download from Google Drive → upload to Azure.
    Returns: (bucket_name, object_path)
    """
    print("Extracting raw file from Google Drive")

    with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as tmp:
        tmp_path = tmp.name
    try:
        gdown.download(f"https://drive.google.com/uc?id={DRIVE_FILE_ID}", tmp_path, quiet=False)

        client = storage.Client()
        bucket = client.bucket(BUCKET_NAME)
        blob = bucket.blob(RAW_OBJECT)
        blob.upload_from_filename(tmp_path, content_type="text/csv")

        print(f"Uploaded {len(tmp_path)} rows to gs://{BUCKET_NAME}/{RAW_OBJECT}")
        return BUCKET_NAME, RAW_OBJECT
    finally:
        try:
            os.remove(tmp_path)
        except Exception:
            pass
        
# --- Small helper: dict → CSV line ---
def dict_to_csv_line(d: dict, header: list) -> str:
    return ",".join(str(d.get(col, "")) for col in header)


# -----------------------------
#  Cleaning Functions
# -----------------------------

def standardize_columns(row):
    """Standardize keys to snake_case."""
    new_row = {}
    for k, v in row.items():
        new_k = (
            k.strip()
             .lower()
             .replace(" ", "_")
        )
        new_k = re.sub(r"[^\w_]", "", new_k)
        new_row[new_k] = v
    return new_row


def parse_datetime(row):
    """Parse date + time into timestamp, date_id."""
    try:
        if "date" in row and "time" in row:
            ts = datetime.strptime(f"{row['date']} {row['time']}", "%Y-%m-%d %H:%M:%S")
        elif "date" in row:
            ts = datetime.strptime(row["date"], "%Y-%m-%d")
        else:
            ts = None
    except Exception:
        ts = None

    row["booking_timestamp"] = ts
    if ts:
        row["date_id"] = int(ts.strftime("%Y%m%d"))
    else:
        row["date_id"] = None
    return row


def normalize_strings(row):
    """Normalize categoricals (status, payment, etc)."""
    if "booking_status" in row:
        row["booking_status"] = str(row["booking_status"]).title().strip()

    if "payment_method" in row:
        pm_map = {"Upi": "UPI", "Debit Card": "Debit Card",
                  "Credit Card": "Credit Card", "Cash": "Cash"}
        row["payment_method"] = pm_map.get(str(row["payment_method"]).strip(), row["payment_method"])
    return row

# -----------------------------
#  Dimensions
# -----------------------------
class AddSurrogateKey(beam.DoFn):
    def process(self, elements, field_name, sk_name):
        for idx, val in enumerate(elements, start=1):
            yield {sk_name: idx, field_name: val}

def build_dim_date(rows):
    return (
        rows
        | "Extract date tuples" >> beam.Map(lambda r: (
            r.get("date_id"),
            {
                "date": r["booking_timestamp"].date() if r.get("booking_timestamp") else None,
                "year": r["booking_timestamp"].year if r.get("booking_timestamp") else None,
                "month": r["booking_timestamp"].month if r.get("booking_timestamp") else None,
                "quarter": (r["booking_timestamp"].month - 1)//3 + 1 if r.get("booking_timestamp") else None,
                "day_of_week": r["booking_timestamp"].strftime("%A") if r.get("booking_timestamp") else None
            }
        ))
        | "Distinct dates" >> beam.CombinePerKey(lambda vals: next(iter(vals)))
        | "Format dim_date" >> beam.Map(lambda kv: {"date_id": kv[0], **kv[1]})
    )

def build_dim_customer(rows):
    return (
        rows
        | "Extract customers" >> beam.Map(lambda r: r.get("customer_id"))
        | "Drop null customers" >> beam.Filter(lambda cid: cid is not None)
        | "Distinct customers" >> beam.Distinct()
        | "Gather customers list" >> beam.combiners.ToList()
        | "Assign customer_sk" >> beam.ParDo(AddSurrogateKey(), field_name="customer_id", sk_name="customer_sk")
    )


def build_dim_vehicle(rows):
    return (
        rows
        | "Extract vehicles" >> beam.Map(lambda r: r.get("vehicle_type"))
        | "Drop null vehicles" >> beam.Filter(lambda v: v is not None)
        | "Distinct vehicles" >> beam.Distinct()
        | "Gather vehicles list" >> beam.combiners.ToList()
        | "Assign vehicle_sk" >> beam.ParDo(AddSurrogateKey(), field_name="vehicle_type", sk_name="vehicle_sk")
    )


def build_dim_location(rows):
    pickup = rows | "Extract pickups" >> beam.Map(lambda r: r.get("pickup_location"))
    drop   = rows | "Extract drops"   >> beam.Map(lambda r: r.get("drop_location"))

    return (
        (pickup, drop)
        | "Merge locations" >> beam.Flatten()
        | "Drop null locations" >> beam.Filter(lambda l: l is not None)
        | "Distinct locations" >> beam.Distinct()
        | "Gather locations list" >> beam.combiners.ToList()
        | "Assign location_sk" >> beam.ParDo(AddSurrogateKey(), field_name="location_name", sk_name="location_sk")
    )


def build_dim_payment(rows):
    return (
        rows
        | "Extract payments" >> beam.Map(lambda r: r.get("payment_method"))
        | "Drop null payments" >> beam.Filter(lambda p: p is not None)
        | "Distinct payments" >> beam.Distinct()
        | "Gather payments list" >> beam.combiners.ToList()
        | "Assign payment_sk" >> beam.ParDo(AddSurrogateKey(), field_name="payment_method", sk_name="payment_sk")
    )

def build_dim_reason(rows):
    cancel = rows | "Extract cancel reasons" >> beam.Map(lambda r: r.get("reason_for_cancelling_by_customer"))
    inc    = rows | "Extract incomplete reasons" >> beam.Map(lambda r: r.get("incomplete_rides_reason"))
    dri    = rows | "Extract driver reasons" >> beam.Map(lambda r: r.get("driver_cancellation_reason"))

    return (
        (cancel, inc, dri)
        | "Merge reasons" >> beam.Flatten()
        | "Drop null reasons" >> beam.Filter(lambda r: r is not None)
        | "Distinct reasons" >> beam.Distinct()
        | "Gather reasons list" >> beam.combiners.ToList()
        | "Assign reason_sk" >> beam.ParDo(AddSurrogateKey(), field_name="reason_text", sk_name="reason_sk")
        | "Add reason type" >> beam.Map(lambda d: {
            **d,
            "type": (
                "incomplete" if "incomplete" in d["reason_text"].lower()
                else "driver_cancellation" if "driver" in d["reason_text"].lower()
                else "customer_cancellation"
            )
        })
    )


# -----------------------------
#  Fact table
# -----------------------------
def build_fact_booking(rows, dim_customer, dim_vehicle, dim_location, dim_payment, dim_reason):
    customer_map = beam.pvalue.AsDict(
        dim_customer | "CustomerMap" >> beam.Map(lambda d: (d["customer_id"], d["customer_sk"]))
    )
    vehicle_map = beam.pvalue.AsDict(
        dim_vehicle | "VehicleMap" >> beam.Map(lambda d: (d["vehicle_type"], d["vehicle_sk"]))
    )
    location_map = beam.pvalue.AsDict(
        dim_location | "LocationMap" >> beam.Map(lambda d: (d["location_name"], d["location_sk"]))
    )
    payment_map = beam.pvalue.AsDict(
        dim_payment | "PaymentMap" >> beam.Map(lambda d: (d["payment_method"], d["payment_sk"]))
    )
    reason_map = beam.pvalue.AsDict(
        dim_reason | "ReasonMap" >> beam.Map(lambda d: (d["reason_text"], d["reason_sk"]))
    )

    def enrich(row, cmap, vmap, lmap, pmap, rmap):
        customer_cancel = row.get("reason_for_cancelling_by_customer") or row.get("reason_for_cancelling")
        driver_cancel = row.get("driver_cancellation_reason")
        incomplete_reason = row.get("incomplete_rides_reason")
        return {
            "booking_id": row.get("booking_id"),
            "date_id": row.get("date_id"),
            "customer_sk": cmap.get(row.get("customer_id")),   
            "vehicle_sk": vmap.get(row.get("vehicle_type")),   
            "pickup_location_sk": lmap.get(row.get("pickup_location")),  
            "drop_location_sk": lmap.get(row.get("drop_location")),      
            "payment_sk": pmap.get(row.get("payment_method")),
            "customer_cancel_reason_sk": rmap.get(customer_cancel) if customer_cancel else None,
            "driver_cancel_reason_sk": rmap.get(driver_cancel) if driver_cancel else None,
            "incomplete_reason_sk": rmap.get(incomplete_reason) if incomplete_reason else None,
            "booking_status": row.get("booking_status"),
            "avg_vtat": row.get("avg_vtat"),
            "avg_ctat": row.get("avg_ctat"),
            "booking_value": row.get("booking_value"),
            "ride_distance": row.get("ride_distance"),
            "driver_ratings": row.get("driver_ratings"),
            "customer_rating": row.get("customer_rating"),
        }

    return rows | "Build FactBooking" >> beam.Map(
        enrich, customer_map, vehicle_map, location_map, payment_map, reason_map
    )

# -----------------------------
# Pipeline
# -----------------------------
def run_pipeline(input_path, output_prefix):
    # Step 0: Check if file exists, otherwise extract and upload data
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)
    blob = bucket.blob(RAW_OBJECT)
    
    if not blob.exists():
        print("File not found in GCS, extracting from Google Drive...")
        extract_and_upload()
    else:
        print(f"File already exists in GCS: gs://{BUCKET_NAME}/{RAW_OBJECT}")
    
    print("Starting ETL Pipeline...")
    
    with beam.Pipeline(options=PipelineOptions()) as p:
        print("Step 1: Reading and cleaning data...")
        rows = (
            p
            | "ReadCSV" >> beam.io.ReadFromText(f"gs://{BUCKET_NAME}/{RAW_OBJECT}", skip_header_lines=1)
            | "ParseCSV" >> beam.Map(lambda line: dict(zip(
                ["date","time","booking_id","booking_status","customer_id","vehicle_type",
                 "pickup_location","drop_location","avg_vtat","avg_ctat","booking_value",
                 "ride_distance","driver_ratings","customer_rating","payment_method",
                 "reason_for_cancelling_by_customer","driver_cancellation_reason","incomplete_rides_reason"],
                line.split(",")
            )))
            | "StandardiseColumns" >> beam.Map(standardize_columns)
            | "ParseDatetime" >> beam.Map(parse_datetime)
            | "NormaliseStrings" >> beam.Map(normalize_strings)
        )

        print("Step 2: Writing cleaned data...")
        header = ["date","time","booking_id","booking_status","customer_id","vehicle_type",
                  "pickup_location","drop_location","avg_vtat","avg_ctat","booking_value",
                  "ride_distance","driver_ratings","customer_rating","payment_method",
                  "reason_for_cancelling_by_customer","driver_cancellation_reason","incomplete_rides_reason",
                  "booking_timestamp","date_id"]
        (
            rows
            | "CleanedToCSV" >> beam.Map(lambda r: ",".join([str(r.get(k,"")) for k in header]))
            | "WriteCleaned" >> beam.io.WriteToText(
                file_path_prefix=f"{output_prefix}/cleaned",
                file_name_suffix=".csv",
                shard_name_template=""
            )
        )

        print("Step 3: Building dimension tables...")
        dim_date     = build_dim_date(rows)
        dim_customer = build_dim_customer(rows)
        dim_vehicle  = build_dim_vehicle(rows)
        dim_location = build_dim_location(rows)
        dim_payment  = build_dim_payment(rows)
        dim_reason   = build_dim_reason(rows)

        print("Step 4: Loading dimensions to BigQuery...")
        dim_date | "BQDimDate" >> beam.io.WriteToBigQuery(
            table=date_table_spec,
            schema="date_id:INTEGER,date:DATE,year:INTEGER,month:INTEGER,quarter:INTEGER,day_of_week:STRING",
            create_disposition="CREATE_IF_NEEDED",
            write_disposition="WRITE_APPEND",
            custom_gcs_temp_location=f"gs://{BUCKET_NAME2}/temp"
        )
        dim_customer | "BQDimCustomer" >> beam.io.WriteToBigQuery(
            table=customer_table_spec,
            schema="customer_sk:INTEGER,customer_id:STRING",
            create_disposition="CREATE_IF_NEEDED",
            write_disposition="WRITE_APPEND",
            custom_gcs_temp_location=f"gs://{BUCKET_NAME2}/temp"
        )
        dim_vehicle | "BQDimVehicle" >> beam.io.WriteToBigQuery(
            table=vehicle_table_spec,
            schema="vehicle_sk:INTEGER,vehicle_type:STRING",
            create_disposition="CREATE_IF_NEEDED",
            write_disposition="WRITE_APPEND",
            custom_gcs_temp_location=f"gs://{BUCKET_NAME2}/temp"
        )
        dim_location | "BQDimLocation" >> beam.io.WriteToBigQuery(
            table=location_table_spec,
            schema="location_sk:INTEGER,location_name:STRING",
            create_disposition="CREATE_IF_NEEDED",
            write_disposition="WRITE_APPEND",
            custom_gcs_temp_location=f"gs://{BUCKET_NAME2}/temp"
        )
        dim_payment | "BQDimPayment" >> beam.io.WriteToBigQuery(
            table=payment_table_spec,
            schema="payment_sk:INTEGER,payment_method:STRING",
            create_disposition="CREATE_IF_NEEDED",
            write_disposition="WRITE_APPEND",
            custom_gcs_temp_location=f"gs://{BUCKET_NAME2}/temp"
        )
        dim_reason | "BQDimReason" >> beam.io.WriteToBigQuery(
            table=reason_table_spec,
            schema="reason_sk:INTEGER,reason_text:STRING,type:STRING",
            create_disposition="CREATE_IF_NEEDED",
            write_disposition="WRITE_APPEND",
            custom_gcs_temp_location=f"gs://{BUCKET_NAME2}/temp"
        )

        print("Step 5: Writing dimensions to GCS...")
        dim_outputs = {
            "dim_date": (["date_id","date","year","month","quarter","day_of_week"], dim_date),
            "dim_customer": (["customer_sk","customer_id"], dim_customer),
            "dim_vehicle": (["vehicle_sk","vehicle_type"], dim_vehicle),
            "dim_location": (["location_sk","location_name"], dim_location),
            "dim_payment": (["payment_sk","payment_method"], dim_payment),
            "dim_reason": (["reason_sk","reason_text","type"], dim_reason),
        }
        for name, (cols, pcoll) in dim_outputs.items():
            (
                pcoll
                | f"{name}ToCSV" >> beam.Map(lambda d: ",".join(str(d.get(c,"")) for c in cols))
                | f"Write{name}" >> beam.io.WriteToText(
                    file_path_prefix=f"{output_prefix}/{name}",
                    file_name_suffix=".csv",
                    shard_name_template=""
                )
            )

        print("Step 6: Building fact table...")
        fact_booking = build_fact_booking(rows, dim_customer, dim_vehicle, dim_location, dim_payment, dim_reason)

        print("Step 7: Loading fact table to BigQuery...")
        fact_booking | "BQFactBooking" >> beam.io.WriteToBigQuery(
            table=fact_table_spec,
            schema=(
                "booking_id:STRING,date_id:INTEGER,customer_sk:INTEGER,"
                "vehicle_sk:INTEGER,pickup_location_sk:INTEGER,drop_location_sk:INTEGER,"
                "payment_sk:INTEGER,customer_cancel_reason_sk:INTEGER,driver_cancel_reason_sk:INTEGER,"
                "incomplete_reason_sk:INTEGER,booking_status:STRING,avg_vtat:STRING,avg_ctat:STRING,"
                "booking_value:STRING,ride_distance:STRING,driver_ratings:STRING,customer_rating:STRING"
            ),
            create_disposition="CREATE_IF_NEEDED",
            write_disposition="WRITE_APPEND",
            custom_gcs_temp_location=f"gs://{BUCKET_NAME2}/temp"
        )

        print("Step 8: Writing fact table to GCS...")
        fact_cols = ["booking_id","date_id","customer_sk","vehicle_sk","pickup_location_sk",
                     "drop_location_sk","payment_sk","customer_cancel_reason_sk","driver_cancel_reason_sk",
                     "incomplete_reason_sk","booking_status","avg_vtat","avg_ctat","booking_value",
                     "ride_distance","driver_ratings","customer_rating"]
        (
            fact_booking
            | "FactBookingToCSV" >> beam.Map(lambda d: ",".join(str(d.get(c,"")) for c in fact_cols))
            | "WriteFactBooking" >> beam.io.WriteToText(
                file_path_prefix=f"{output_prefix}/fact_booking",
                file_name_suffix=".csv",
                shard_name_template=""
            )
        )
    
    print("ETL Pipeline completed successfully!")



if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Uber ETL Pipeline with Apache Beam')
    parser.add_argument(
        '--input',
        default=f"gs://{BUCKET_NAME}/{RAW_OBJECT}",
        help='Input GCS path (default: from env vars)'
    )
    parser.add_argument(
        '--output-prefix',
        default=f"gs://{BUCKET_NAME2}/{OUT_PREFIX}",
        help='Output GCS prefix (default: from env vars)'
    )
    parser.add_argument(
        '--project-id',
        default=PROJECT_ID,
        help='GCP Project ID (default: from env vars)'
    )
    parser.add_argument(
        '--dataset',
        default=BIGQUERY_DATASET,
        help='BigQuery dataset (default: from env vars)'
    )
    parser.add_argument(
        '--extract',
        action='store_true',
        help='Force extract from Google Drive and upload to GCS'
    )
    
    args = parser.parse_args()
    output_prefix = args.output_prefix.rstrip("/")

    if args.extract:
        print("Forcing extraction from Google Drive...")
        extract_and_upload()

    run_pipeline(args.input, output_prefix)
