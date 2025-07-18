import boto3
import csv
import mysql.connector

def delete_files_with_prefix(bucket_name, rootname, s3):
    prefix = f"{rootname}_"
    print(f"\n🔍 Searching for S3 files with prefix: '{prefix}'")

    paginator = s3.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=bucket_name, Prefix=prefix)

    keys_to_delete = []

    for page in pages:
        if 'Contents' in page:
            for obj in page['Contents']:
                print(f"   📁 Found matching object: {obj['Key']} (LastModified: {obj['LastModified']})")
                keys_to_delete.append({'Key': obj['Key']})

    if not keys_to_delete:
        print(f"⚠️ No S3 files found for prefix: '{prefix}'")
        return

    print(f"🗑 Preparing to delete {len(keys_to_delete)} S3 files for prefix: '{prefix}'")

    for i in range(0, len(keys_to_delete), 1000):
        chunk = keys_to_delete[i:i + 1000]
        response = s3.delete_objects(
            Bucket=bucket_name,
            Delete={'Objects': chunk}
        )
        deleted = response.get("Deleted", [])
        print(f"✅ Deleted {len(deleted)} objects in this batch.")
        for obj in deleted:
            print(f"   ✅ Deleted: {obj['Key']}")

def delete_db_rows_for_campaign_id(db_connection, rootname):
    try:
        cursor = db_connection.cursor()
        delete_query = "DELETE FROM mobile_metadata WHERE campaign_id = %s"
        cursor.execute(delete_query, (rootname,))
        db_connection.commit()
        print(f"✅ Deleted {cursor.rowcount} rows from mobile_metadata where campaign_id = '{rootname}'")
    except mysql.connector.Error as err:
        print(f"❌ MySQL error for campaign_id '{rootname}': {err}")
    finally:
        cursor.close()

def main():
    csv_file = "StaleCampaignsToDelete.csv"
    bucket_name = "urban-heat-island-data"

    # Initialize S3 client
    try:
        s3 = boto3.client('s3')
        print("✅ Connected to AWS S3")
    except Exception as e:
        print(f"❌ Failed to initialize S3 client: {e}")
        return

    # Connect to MySQL
    try:
        db_connection = mysql.connector.connect(
            host="localhost",     # Adjust if not local
            user="uhi",
            password="uhi",
            database="uhi"
        )
        print("✅ Connected to MySQL database 'uhi'")
    except mysql.connector.Error as err:
        print(f"❌ Failed to connect to MySQL: {err}")
        return

    # Process each rootname from CSV
    try:
        with open(csv_file, newline='') as csvfile:
            reader = csv.reader(csvfile)
            for row in reader:
                if row:
                    raw_rootname = row[0]
                    rootname = raw_rootname.strip()
                    print(f"\n📦 Processing rootname -> raw: {repr(raw_rootname)}, stripped: {repr(rootname)}")

                    if rootname:
                        delete_files_with_prefix(bucket_name, rootname, s3)
                        delete_db_rows_for_campaign_id(db_connection, rootname)
                        print(f"🔄 Finished processing for campaign_id '{rootname}'")
                    else:
                        print("⚠️ Skipping empty rootname after strip.")
    except FileNotFoundError:
        print(f"❌ CSV file not found: {csv_file}")
    except Exception as e:
        print(f"❌ Unexpected error while processing CSV: {e}")

    db_connection.close()
    print("\n✅ Done.")

if __name__ == "__main__":
    main()
