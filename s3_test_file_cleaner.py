import boto3
import json
import sys
import os
import argparse
import mysql.connector
from botocore.exceptions import ClientError

# --- Configuration ---
LOCATIONS_BUCKET = 'uhi-locations'
LOCATIONS_FILE = 'locations.json'

# --- DB Configuration (reads from environment variables) ---
DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_USER = os.getenv('DB_USER')
DB_PASS = os.getenv('DB_PASSWORD')
DB_NAME = 'uhi'
DB_TABLE = 'mobile_metadata'
DB_COLUMN = 'campaign_id'
# ---------------------

def parse_args():
    """
    Parses command-line arguments.
    """
    parser = argparse.ArgumentParser(
        description='Clean UHI test data from S3 buckets and the MySQL database.'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        default=False,
        help='Preview what would be deleted without making any changes. (default: False)'
    )
    parser.add_argument(
        '--target-word',
        type=str,
        default='test',
        help='The word to search for when identifying files/rows to delete. (default: "test")'
    )
    return parser.parse_args()


def clean_database(target_word, dry_run):
    """
    Connects to the 'uhi' MySQL database and deletes rows from
    'mobile_metadata' where the 'campaign_id' contains the target_word
    (case-insensitive). In dry-run mode, reports matches without deleting.
    """
    print(f"Attempting to clean database '{DB_NAME}'...")

    if not DB_USER or not DB_PASS:
        print(f"Error: DB_USER or DB_PASS environment variables are not set.", file=sys.stderr)
        print("Skipping database cleaning step.")
        return

    connection = None
    cursor = None
    try:
        connection = mysql.connector.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASS,
            database=DB_NAME
        )

        if connection.is_connected():
            print(f"Successfully connected to database '{DB_NAME}' at {DB_HOST}.")

            cursor = connection.cursor()
            like_value = (f'%{target_word}%',)

            if dry_run:
                # In dry-run mode, SELECT instead of DELETE
                query = f"SELECT {DB_COLUMN} FROM {DB_TABLE} WHERE LOWER({DB_COLUMN}) LIKE %s"
                cursor.execute(query, like_value)
                rows = cursor.fetchall()
                print(f"[DRY RUN] Would delete {len(rows)} rows from '{DB_TABLE}':")
                for row in rows:
                    print(f"  [WOULD DELETE] {DB_COLUMN}={row[0]}")
            else:
                query = f"DELETE FROM {DB_TABLE} WHERE LOWER({DB_COLUMN}) LIKE %s"
                cursor.execute(query, like_value)
                connection.commit()
                print(f"Database cleanup complete. {cursor.rowcount} rows deleted from '{DB_TABLE}'.")

    except mysql.connector.Error as err:
        print(f"Error connecting to or updating MySQL database: {err}", file=sys.stderr)
        if connection:
            connection.rollback()
    except Exception as e:
        print(f"An unexpected error occurred during database cleaning: {e}", file=sys.stderr)
    finally:
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


def find_and_delete_test_files(s3_client, bucket_name, target_word, dry_run):
    """
    Lists all objects in a given bucket and deletes any containing target_word
    (case-insensitive). In dry-run mode, reports matches without deleting.
    """
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

            if target_word in key.lower():
                objects_to_delete.append({'Key': key})
                action = "[WOULD DELETE]" if dry_run else "[MARK]"
                print(f"  {action} {key}")

    print(f"Scanned {total_files_scanned} files.")

    if not objects_to_delete:
        print(f"No files containing '{target_word}' found in this bucket.")
        return

    print(f"Found {len(objects_to_delete)} files to delete.")

    if dry_run:
        print(f"[DRY RUN] Skipping deletion of {len(objects_to_delete)} files in '{bucket_name}'.")
        return

    # 2. Delete the files in batches of 1000
    for i in range(0, len(objects_to_delete), 1000):
        chunk = objects_to_delete[i:i + 1000]

        print(f"Deleting batch of {len(chunk)} files...")

        try:
            response = s3_client.delete_objects(
                Bucket=bucket_name,
                Delete={'Objects': chunk}
            )

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
    args = parse_args()
    dry_run = args.dry_run
    target_word = args.target_word.lower()

    if dry_run:
        print("*** DRY RUN MODE — no changes will be made ***\n")

    print(f"Target word: '{target_word}'")

    # --- Step 1: Process S3 Buckets ---
    print("\nStep 1: Processing S3 Buckets")
    s3_client = boto3.client('s3')

    locations = get_locations(s3_client)

    if not locations:
        print("No S3 locations to process. Skipping S3 cleanup.")
    else:
        print("\n" + "="*50)

        for location in locations:
            bucket_name = location.get('value')
            label = location.get('label', 'N/A')

            if not bucket_name:
                print(f"Skipping location with no 'value' field: {location}")
                continue

            print(f"\nProcessing Location: {label} (Bucket: {bucket_name})")

            try:
                find_and_delete_test_files(s3_client, bucket_name, target_word, dry_run)

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
        clean_database(target_word, dry_run)
    except Exception as e:
        print(f"A critical error occurred during database cleaning: {e}", file=sys.stderr)
    print("\n" + "="*50)

    print("\nAll tasks processed.")


if __name__ == "__main__":
    main()
