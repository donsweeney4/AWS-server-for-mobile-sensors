import boto3
import mysql.connector
import os
import re
import sys
import json  # Added for parsing JSON
from datetime import datetime

# === Configurable ===
LOGFILE = "/home/ubuntu/HeatIslandResultsServer/crontab.log"
LOCATIONS_BUCKET = 'uhi-locations'  # Bucket containing the locations file
LOCATIONS_FILE = 'locations.json'   # The locations file itself
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
LOCATION_COLUMN_NAME = 'campaign_location' # New column for location

############################################################################
# Function to log messages to a file with timestamps##########
#def log(msg):
#    timestamp = datetime.now().strftime("[%Y-%m-%d %H:%M:%S]")
#    with open(LOGFILE, "a") as f:
#        f.write(f"{timestamp} {msg}\n")
############################################################################
# Function to log messages to the terminal without a timestamp
def log(msg):
    # Now it just prints to the terminal
    print(f" {msg}")


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
# Function to modify filenames by removing the trailing _###.csv and truncating
def modify_filename(filename):
    match = re.match(r'^(.*)_\d{3}\.csv$', filename)
    if match:
        remaining_match = match.group(1)
        truncated_match = remaining_match[:20]
        return truncated_match
    else:
        return None

######################################################################################
# Function to get the list of locations from the JSON file in S3
def get_locations():
    s3 = boto3.client('s3')
    try:
        response = s3.get_object(Bucket=LOCATIONS_BUCKET, Key=LOCATIONS_FILE)
        content = response['Body'].read().decode('utf-8')
        locations = json.loads(content)
        
        if not isinstance(locations, list):
            raise ValueError(f"{LOCATIONS_FILE} does not contain a JSON list.")
            
        log(f"Found {len(locations)} locations from {LOCATIONS_BUCKET}/{LOCATIONS_FILE}.")
        return locations
    except Exception as e:
        raise RuntimeError(f"Failed to get or parse {LOCATIONS_FILE}: {e}")

######################################################################################
# S3 operations to get all filenames in a specified bucket
def get_s3_filenames(bucket_name):
    s3 = boto3.client('s3')
    try:
        response = s3.list_objects_v2(Bucket=bucket_name)
        return [item['Key'] for item in response.get('Contents', [])]
    except Exception as e:
        # Log bucket-specific error, but raise it so the loop can catch it
        raise RuntimeError(f"S3 list error for bucket '{bucket_name}': {e}")

######################################################################################
# Database operations to find all distinct campaign_ids for a specific location
def get_distinct_campaign_ids_from_db(location_value):
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()
        # MODIFIED: Added WHERE clause for campaign_location
        query = f"SELECT DISTINCT {COLUMN_NAME} FROM {TABLE_NAME} WHERE {LOCATION_COLUMN_NAME} = %s"
        cursor.execute(query, (location_value,)) # Pass location as parameter
        results = {row[0] for row in cursor.fetchall()}
        return results
    except mysql.connector.Error as e:
        raise RuntimeError(f"MySQL query error for location '{location_value}': {e}")
    finally:
        if 'cursor' in locals(): cursor.close()
        if 'conn' in locals() and conn.is_connected(): conn.close()

######################################################################################
# Function to insert new campaign_ids into the mysql table with their location
def insert_new_campaign_ids(new_ids, location_value):
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()
        # MODIFIED: Insert both campaign_id and campaign_location
        sql = f"INSERT IGNORE INTO {TABLE_NAME} ({COLUMN_NAME}, {LOCATION_COLUMN_NAME}) VALUES (%s, %s)"
        
        # Create a list of tuples: (campaign_id, location_value)
        values = [(cid, location_value) for cid in new_ids]
        log(" ")
        log(f"Executing SQL: {sql} with values: {values}")
        cursor.executemany(sql, values)
        conn.commit()
        log(f"✅ Inserted {cursor.rowcount} new campaign_id(s) for location '{location_value}'.")
    except mysql.connector.Error as e:
        raise RuntimeError(f"Insert failed for location '{location_value}': {e}")
    finally:
        if 'cursor' in locals(): cursor.close()
        if 'conn' in locals() and conn.is_connected(): conn.close()

######################################################################################
# Main function definition run as a cron job once every minute
def main():
    log("🔁 Cron sync started.")
    
    # 1. Get the list of all locations
    try:
        locations = get_locations()
    except Exception as e:
        # If this fails, we can't do anything else. Log, email, and exit.
        log(f"❌ CRITICAL: Could not retrieve {LOCATIONS_FILE}. Error: {e}")
        raise # Re-raise the exception to trigger the email alert in __main__

    if not locations:
        log("No locations found in locations.json. Nothing to process.")
        log("✅ Cron sync complete.\n")
        return

    # 2. Loop through each location and process its bucket
    for location in locations:
        try:
            location_value = location.get('value')
            location_label = location.get('label', 'N/A') # For logging
            
            if not location_value:
                log(" ")
                log(f"⚠️ Skipping location with no 'value' field: {location}")
                continue
            log(" ")
            log(" ")
            log(f"--- Processing location: {location_label} (Bucket: {location_value}) ---")

            # 3. Get filenames from the location-specific bucket
            filenames = get_s3_filenames(location_value)
            log(" ")
            log(f"Found {len(filenames)} file(s) in S3 bucket '{location_value}'.")

            # 4. Modify and get distinct IDs
            modified = [name for name in (modify_filename(f) for f in filenames) if name is not None]
            distinct_s3_ids = set(modified)
            log(f"Filtered to {len(distinct_s3_ids)} unique campaign names from S3.")

            # 5. Get existing DB IDs *for this location only*
            existing_ids = get_distinct_campaign_ids_from_db(location_value)
            log(f"Found {len(existing_ids)} campaign_id(s) already in DB for '{location_value}'.")

            # 6. Find and insert new IDs *for this location*
            new_ids = sorted(distinct_s3_ids - existing_ids)
            if new_ids:
                log(f"Discovered {len(new_ids)} new campaign_id(s): {', '.join(new_ids)}")
                insert_new_campaign_ids(new_ids, location_value)
            else:
                log(f"✅ No new campaign_id(s) to insert for '{location_value}'.")

        except Exception as e:
            # If one location fails, log it, email about it, but continue to the next location
            error_message = f"❌ Failed to process location '{location.get('value', 'UNKNOWN')}': {str(e)}"
            log(error_message)
            send_error_email(subject=f"🚨 Cron Job Failure for location '{location.get('value', 'UNKNOWN')}'", body=error_message)
            # Continue to the next location in the loop

    log("✅ Cron sync complete.\n")

######################################################################################
# Run locally
if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        # This will catch critical failures, like failing to get locations.json
        error_message = f"❌ Cron job failed critically: {str(e)}"
        log(error_message)
        send_error_email(subject="🚨 Cron Job CRITICAL Failure on EC2", body=error_message)
        sys.exit(1)

    """
    To run this script manually for testing, use:

export DB_HOST=localhost
export DB_USER=uhi
export DB_PASSWORD=uhi
    
    """    