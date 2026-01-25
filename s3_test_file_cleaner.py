import boto3
import json
import sys
import os  # Added for environment variables
import mysql.connector  # Added for MySQL
from botocore.exceptions import ClientError

# --- Configuration ---
LOCATIONS_BUCKET = 'uhi-locations'
LOCATIONS_FILE = 'locations.json'
TARGET_WORD = 'test'

# --- DB Configuration (reads from environment variables) ---
DB_HOST = os.getenv('DB_HOST', 'localhost') # Default to localhost if not set
DB_USER = os.getenv('DB_USER')
DB_PASS = os.getenv('DB_PASSWORD')
DB_NAME = 'uhi'
DB_TABLE = 'mobile_metadata'
DB_COLUMN = 'campaign_id'
# ---------------------

def clean_database():
    """
    Connects to the 'uhi' MySQL database and deletes rows from
    'mobile_metadata' where the 'campaign_id' contains 'test'
    (case-insensitive).
    """
    print(f"Attempting to clean database '{DB_NAME}'...")

    # Check for required environment variables
    if not DB_USER or not DB_PASS:
        print(f"Error: DB_USER or DB_PASS environment variables are not set.", file=sys.stderr)
        print("Skipping database cleaning step.")
        return

    connection = None
    cursor = None
    try:
        # Establish the database connection
        connection = mysql.connector.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASS,
            database=DB_NAME
        )
        
        if connection.is_connected():
            print(f"Successfully connected to database '{DB_NAME}' at {DB_HOST}.")
            
            cursor = connection.cursor()
            
            # SQL query to delete rows. 
            # LOWER() makes the check case-insensitive.
            # LIKE %...% finds 'test' anywhere in the string.
            query = f"DELETE FROM {DB_TABLE} WHERE LOWER({DB_COLUMN}) LIKE %s"
            value = ('%test%',) # The value to search for
            
            cursor.execute(query, value)
            
            # Commit the transaction
            connection.commit()
            
            print(f"Database cleanup complete. {cursor.rowcount} rows deleted from '{DB_TABLE}'.")

    except mysql.connector.Error as err:
        print(f"Error connecting to or updating MySQL database: {err}", file=sys.stderr)
        if connection:
            connection.rollback() # Rollback changes on error
    except Exception as e:
        print(f"An unexpected error occurred during database cleaning: {e}", file=sys.stderr)
    finally:
        # Ensure connection and cursor are closed
        if cursor:
            cursor.close()
        if connection and connection.is_connected():
            connection.close()
            print("Database connection closed.")

def get_locations(s3_client):
    """
    Fetches and parses the locations.json file from the uhi-locations bucket.
    """
    print(f"Fetching locations from: s3://{LOCATIONS_BUCKET}/{LOCATIONS_FILE}")
    try:
        response = s3_client.get_object(Bucket=LOCATIONS_BUCKET, Key=LOCATIONS_FILE)
        content = response['Body'].read().decode('utf-8')
        locations = json.loads(content)
        
        if not isinstance(locations, list):
            print(f"Error: {LOCATIONS_FILE} does not contain a JSON list.", file=sys.stderr)
            return None
            
        print(f"Found {len(locations)} locations to process.")
        return locations
        
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchBucket':
            print(f"Error: The bucket '{LOCATIONS_BUCKET}' does not exist.", file=sys.stderr)
        elif e.response['Error']['Code'] == 'NoSuchKey':
            print(f"Error: The file '{LOCATIONS_FILE}' does not exist in the bucket.", file=sys.stderr)
        else:
            print(f"Boto3 ClientError: {e}", file=sys.stderr)
        return None
    except json.JSONDecodeError:
        print(f"Error: Failed to decode JSON from {LOCATIONS_FILE}.", file=sys.stderr)
        return None
    except Exception as e:
        print(f"An unexpected error occurred getting locations: {e}", file=sys.stderr)
        return None

def find_and_delete_test_files(s3_client, bucket_name):
    """
    Lists all objects in a given bucket and deletes any containing 'test' (case-insensitive).
    """
    
    # Use a paginator to handle buckets with more than 1000 objects
    paginator = s3_client.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=bucket_name)

    objects_to_delete = []
    total_files_scanned = 0

    print("Scanning for files to delete...")
    
    # 1. Find all matching files
    for page in pages:
        if 'Contents' not in page:
            continue
            
        for obj in page['Contents']:
            total_files_scanned += 1
            key = obj['Key']
            
            # Case-insensitive check
            if TARGET_WORD in key.lower():
                objects_to_delete.append({'Key': key})
                print(f"  [MARK] {key}")

    print(f"Scanned {total_files_scanned} files.")

    # 2. Delete the files in batches of 1000
    if not objects_to_delete:
        print(f"No files containing '{TARGET_WORD}' found in this bucket.")
        return

    print(f"Found {len(objects_to_delete)} files to delete.")
    
    # S3 delete_objects can only handle 1000 keys at a time.
    # We must chunk our list into batches.
    for i in range(0, len(objects_to_delete), 1000):
        chunk = objects_to_delete[i:i + 1000]
        
        print(f"Deleting batch of {len(chunk)} files...")
        
        try:
            response = s3_client.delete_objects(
                Bucket=bucket_name,
                Delete={'Objects': chunk}
            )
            
            # Log any errors from the batch delete
            if 'Errors' in response:
                for error in response['Errors']:
                    print(f"  [FAIL] Could not delete {error['Key']}: {error['Message']}", file=sys.stderr)
                    
        except ClientError as e:
            print(f"Error during batch deletion: {e}", file=sys.stderr)
            
    print(f"Deletion process complete for bucket '{bucket_name}'.")

def main():
    """
    Main execution function.
    """

    # --- Step 1: Process S3 Buckets ---
    print("Step 1: Processing S3 Buckets")
    s3_client = boto3.client('s3')
    
    locations = get_locations(s3_client)
    
    if not locations:
        print("No S3 locations to process. Skipping S3 cleanup.")
    else:
        print("\n" + "="*50)

        # Loop through each location found in the JSON file
        for location in locations:
            bucket_name = location.get('value')
            label = location.get('label', 'N/A')
            
            if not bucket_name:
                print(f"Skipping location with no 'value' field: {location}")
                continue
                
            print(f"\nProcessing Location: {label} (Bucket: {bucket_name})")
            
            try:
                find_and_delete_test_files(s3_client, bucket_name)
                
            except ClientError as e:
                if e.response['Error']['Code'] == 'NoSuchBucket':
                    print(f"Error: The bucket '{bucket_name}' does not exist. Skipping.", file=sys.stderr)
                else:
                    print(f"An unexpected error occurred processing {bucket_name}: {e}", file=sys.stderr)
            except Exception as e:
                print(f"A general error occurred for {bucket_name}: {e}", file=sys.stderr)
                
            print("="*50)
        
        print("\nS3 processing complete.")

    # --- Step 2: Clean Database ---
    print("\n" + "="*50)
    print("Step 2: Cleaning Database")
    try:
        clean_database()
    except Exception as e:
        print(f"A critical error occurred during database cleaning: {e}", file=sys.stderr)
    print("\n" + "="*50)
    # -------------------------

    print("\nAll tasks processed.")

if __name__ == "__main__":
    main()