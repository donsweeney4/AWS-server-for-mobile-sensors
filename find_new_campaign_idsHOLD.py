import boto3
import mysql.connector
import os
import re
import sys
from datetime import datetime

# === Configurable ===
LOGFILE = "/home/ubuntu/HeatIslandResultsServer/crontab.log"
BUCKET_NAME = 'urban-heat-island-data'
EMAIL_TO = "donsweeney4@gmail.com"
EMAIL_FROM = "donsweeney4@gmail.com"  # Must be verified in SES
AWS_REGION = "us-west-2"

DB_CONFIG = {
    'host': os.environ.get('DB_HOST', 'localhost'),
    'user': os.environ.get('DB_USER', 'youruser'),
    'password': os.environ.get('DB_PASSWORD', 'yourpass'),
    'database': 'uhi'
}

TABLE_NAME = 'mobile_metadata'
COLUMN_NAME = 'campaign_id'

######################################################################################
# Function to log messages to a file with timestamps
def log(msg):
    timestamp = datetime.now().strftime("[%Y-%m-%d %H:%M:%S]")
    with open(LOGFILE, "a") as f:
        f.write(f"{timestamp} {msg}\n")

######################################################################################
# Function to send an email using AWS SES
def send_error_email(subject, body):
    try:
        client = boto3.client('ses', region_name=AWS_REGION)
        client.send_email(
            Source=EMAIL_FROM,
            Destination={'ToAddresses': [EMAIL_TO]},
            Message={
                'Subject': {'Data': subject},
                'Body': {'Text': {'Data': body}}
            }
        )
    except Exception as e:
        log(f"🚨 Failed to send SES email: {e}")

######################################################################################
# Function to modify filenames by removing the trailing _###.csv and truncating the name to the first 20 characters
def modify_filename(filename):
    match = re.match(r'^(.*)_\d{3}\.csv$', filename)
    if match:
        # Take the remaining match
        remaining_match = match.group(1)
        
        # Truncate it to the first 20 characters
        truncated_match = remaining_match[:20]
        
        return truncated_match
    else:
        return None


######################################################################################
# S3 operations to get all filenames in the specified bucket
def get_s3_filenames(bucket_name):
    s3 = boto3.client('s3')
    try:
        response = s3.list_objects_v2(Bucket=bucket_name)
        return [item['Key'] for item in response.get('Contents', [])]
    except Exception as e:
        raise RuntimeError(f"S3 list error: {e}")

######################################################################################
# Database operations to find all distinct campaign_ids in the specified mysql table
def get_distinct_campaign_ids_from_db():
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()
        query = f"SELECT DISTINCT {COLUMN_NAME} FROM {TABLE_NAME}"
        cursor.execute(query)
        results = {row[0] for row in cursor.fetchall()}
        return results
    except mysql.connector.Error as e:
        raise RuntimeError(f"MySQL query error: {e}")
    finally:
        if 'cursor' in locals(): cursor.close()
        if 'conn' in locals() and conn.is_connected(): conn.close()

######################################################################################
# Function to insert new campaign_ids into the mysql table
def insert_new_campaign_ids(new_ids):
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()
        sql = f"INSERT IGNORE INTO {TABLE_NAME} ({COLUMN_NAME}) VALUES (%s)"
        values = [(cid,) for cid in new_ids]
        cursor.executemany(sql, values)
        conn.commit()
        log(f"✅ Inserted {cursor.rowcount} new campaign_id(s).")
    except mysql.connector.Error as e:
        raise RuntimeError(f"Insert failed: {e}")
    finally:
        if 'cursor' in locals(): cursor.close()
        if 'conn' in locals() and conn.is_connected(): conn.close()

######################################################################################
# Main function definition run as a cron job once every minute
def main():
    log("🔁 Cron sync started.")
    filenames = get_s3_filenames(BUCKET_NAME)
    log(f"Found {len(filenames)} file(s) in S3.")

    modified = [name for name in (modify_filename(f) for f in filenames) if name is not None]
    distinct_s3_ids = set(modified)
    log(f"Filtered to {len(distinct_s3_ids)} unique campaign names from S3.")

    existing_ids = get_distinct_campaign_ids_from_db()
    log(f"Found {len(existing_ids)} campaign_id(s) already in DB.")

    new_ids = sorted(distinct_s3_ids - existing_ids)
    if new_ids:
        log(f"Discovered {len(new_ids)} new campaign_id(s): {', '.join(new_ids)}")
        insert_new_campaign_ids(new_ids)
    else:
        log("✅ No new campaign_id(s) to insert.")

    log("✅ Cron sync complete.\n")

######################################################################################
# Run locally
if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        error_message = f"❌ Cron job failed: {str(e)}"
        log(error_message)
        send_error_email(subject="🚨 Cron Job Failure on EC2", body=error_message)
        sys.exit(1)
