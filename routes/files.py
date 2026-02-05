import os
import io
import zipfile
import json
import logging
import asyncio
import re
from quart import Blueprint, jsonify, request, Response, session, send_file, current_app
from botocore.exceptions import ClientError

# Define blueprint
files_bp = Blueprint('files', __name__)

logger = logging.getLogger(__name__)

# --- S3 Helper Functions ---
# (These helpers are correct, no changes needed)

async def list_s3_files(bucket_name):
    """Lists all objects (files) in a given S3 bucket."""
    s3_client = current_app.s3_client
    if not s3_client:
        return {"error": "S3 client not initialized."}, 500
    if not bucket_name:
        return {"error": "S3 bucket name not configured."}, 500

    try:
        response = await asyncio.to_thread(
            s3_client.list_objects_v2, Bucket=bucket_name
        )
        files = [obj['Key'] for obj in response.get('Contents', [])]
        return files, 200
    except ClientError as e:
        error_code = e.response.get("Error", {}).get("Code")
        if error_code == 'NoSuchBucket':
            return {"error": f"Bucket '{bucket_name}' not found."}, 404
        elif error_code == 'AccessDenied':
            return {"error": "Access denied to S3 bucket. Check credentials/permissions."}, 403
        else:
            return {"error": f"S3 client error: {e}"}, 500
    except Exception as e:
        return {"error": f"An unexpected error occurred while listing S3 files: {e}"}, 500

async def download_s3_file(bucket_name, file_key):
    """Downloads a single file from S3 and returns its content as bytes."""
    s3_client = current_app.s3_client
    if not s3_client:
        raise Exception("S3 client not initialized.")
    if not bucket_name:
        raise Exception("S3 bucket name not configured.")

    try:
        response = await asyncio.to_thread(
            s3_client.get_object, Bucket=bucket_name, Key=file_key
        )
        file_content = await asyncio.to_thread(response['Body'].read)
        return file_content
    except ClientError as e:
        error_code = e.response.get("Error", {}).get("Code")
        if error_code == 'NoSuchKey':
            raise FileNotFoundError(f"File '{file_key}' not found in bucket '{bucket_name}'.")
        elif error_code == 'AccessDenied':
            raise PermissionError(f"Access denied to file '{file_key}'. Check permissions.")
        else:
            raise Exception(f"S3 download error for '{file_key}': {e}")
    except Exception as e:
        raise Exception(f"An unexpected error occurred while downloading '{file_key}': {e}")

# --- S3/File Routes ---

@files_bp.route("/get_presigned_url", methods=["POST"])
async def get_presigned_url():
    """
    Generates a pre-signed S3 PUT URL for uploading CSV files.
    """
    s3_client = current_app.s3_client
    s3_region = current_app.config.get('S3_REGION', 'us-west-2')

    try:
      
        # 1. Get the JSON body *first*
        data = await request.get_json()
        
        # 2. Get the filename from the JSON
        filename = data.get("filename")

        # 3. Get the bucket name from the JSON (this is what the app is sending)
        bucket_name = data.get("bucket")
       

        # 4. Now, validate the inputs
        if not bucket_name:
            logger.error("❌ 'bucket' (location name) missing from request body.")
            return jsonify({"error": "'bucket' is missing from request body."}), 400
        
        if not filename or not re.match(r'^.+_\d{3}\.csv$', filename):
            logger.warning(f"Invalid filename format received: {filename}")
            return jsonify({"error": "Filename must end with _NNN.csv where NNN is a 3-digit number."}), 400

        # --- The rest of your code is correct ---
        s3_key = filename

        # Use asyncio.to_thread for the blocking boto3 call
        presigned_url = await asyncio.to_thread(
            s3_client.generate_presigned_url,
            ClientMethod="put_object",
            Params={
                "Bucket": bucket_name, # <-- This now uses the correct variable
                "Key": s3_key,
                "ContentType": "text/csv"
            },
            ExpiresIn=600  # 10 minutes
        )

        public_url = f"https://{bucket_name}.s3.{s3_region}.amazonaws.com/{s3_key}"
        logger.info(f"Generated presigned URL for {filename} in bucket {bucket_name}")

        return jsonify({
            "uploadUrl": presigned_url,
            "publicUrl": public_url
        })

    except Exception as e:
        logger.exception("Error generating pre-signed URL")
        return jsonify({"error": str(e)}), 500







##//#############################################################################
@files_bp.route('/map')
async def map_view():
    """Fetches and renders the map HTML from S3 for the selected campaign."""
    campaign_id = session.get('campaign_id')
    logger.info(f"In map_view function: Fetching map for campaign_id: {campaign_id}")

    if not campaign_id:
        logger.error("❌ Campaign ID missing from session for map view.")
        return Response("Campaign ID is missing. Please select a campaign.", status=400, content_type='text/plain')

    # --- FIX ---
    # Get the bucket name from the SESSION, just like processing.py
    bucket_name = session.get('selected_location')
    if not bucket_name:
        logger.error("❌ Location (bucket name) missing from session for map view.")
        return Response("Location is missing from session. Please select a location first.", status=400, content_type='text/plain')
    # --- END FIX ---

    key = f"{campaign_id}_color_coded_temperature_map.html"

    try:
        # Use the async helper function to download the file
        file_content_bytes = await download_s3_file(bucket_name, key)
        html_content = file_content_bytes.decode('utf-8')
        
        logger.info(f"Fetched {key} from {bucket_name} successfully.")
        return Response(html_content, content_type='text/html')
    except FileNotFoundError:
        logger.error(f"Failed to fetch {key} from S3: File not found.")
        return Response(f"Could not load map HTML: File not found in bucket {bucket_name}.", status=404, content_type='text/plain')
    except Exception as e:
        logger.error(f"Failed to fetch {key} from S3: {e}")
        return Response(f"Could not load map HTML for campaign {campaign_id}. Details: {e}", status=500, content_type='text/plain')

##//#############################################################################
@files_bp.route('/temperatureplot')
async def temperature_plot():
    """Fetches and renders the temperature plot HTML from S3 for the selected campaign."""
    campaign_id = session.get('campaign_id')
    logger.info(f"In temperature_plot function: Fetching plot for campaign_id: {campaign_id}")

    if not campaign_id:
        logger.error("❌ Campaign ID missing from session for temperature plot.")
        return Response("Campaign ID is missing. Please select a campaign.", status=400, content_type='text/plain')

    # --- FIX ---
    # Get the bucket name from the SESSION, just like processing.py
    bucket_name = session.get('selected_location')
    if not bucket_name:
        logger.error("❌ Location (bucket name) missing from session for temperature plot.")
        return Response("Location is missing from session. Please select a location first.", status=400, content_type='text/plain')
    # --- END FIX ---
    
    key = f"{campaign_id}_fig1_corrected_temperature_map_time_window.html"

    try:
        # Use the async helper function to download the file
        file_content_bytes = await download_s3_file(bucket_name, key)
        html_content = file_content_bytes.decode('utf-8')

        logger.info(f"Fetched {key} from {bucket_name} successfully.")
        return Response(html_content, content_type='text/html')
    except FileNotFoundError:
        logger.error(f"Failed to fetch {key} from S3: File not found.")
        return Response(f"Could not load temperature plot HTML: File not found in bucket {bucket_name}.", status=404, content_type='text/plain')
    except Exception as e:
        logger.error(f"Failed to fetch {key} from S3: {e}")
        return Response(f"Could not load temperature plot HTML for campaign {campaign_id}. Details: {e}", status=500, content_type='text/plain')

##//#############################################################################
@files_bp.route('/get_locations', methods=['GET'])
async def get_locations():
    """
    Fetches the list of locations from the 'locations' config bucket.
    """
    logger.info("Fetching locations from S3...")
    
    # This route is special: it reads from the *static* LOCATIONS bucket
    bucket_name = current_app.config.get('S3_BUCKET_LOCATIONS', 'uhi-locations')
    file_key = "locations.json"

    if not current_app.s3_client:
        logger.error("S3 client not initialized.")
        return jsonify({"error": "S3 client not initialized."}), 500

    try:
        file_content_bytes = await download_s3_file(bucket_name, file_key)
        file_content_str = file_content_bytes.decode('utf-8')
        locations_data = json.loads(file_content_str)
        
        logger.info(f"Successfully fetched and parsed {file_key} from {bucket_name}.")
        return jsonify(locations_data)
    except FileNotFoundError:
        logger.error(f"Location file '{file_key}' not found in bucket '{bucket_name}'.")
        return jsonify({"error": f"Location file not found: {file_key}"}), 404
    except PermissionError:
        logger.error(f"Access denied for S3 file '{file_key}'.")
        return jsonify({"error": "Access denied to location file."}), 403
    except json.JSONDecodeError:
        logger.error(f"Failed to decode JSON from file: {file_key}")
        return jsonify({"error": "Invalid location file format."}), 500
    except Exception as e:
        logger.exception(f"An unexpected error occurred while fetching locations: {e}")
        return jsonify({"error": f"An unexpected error occurred: {str(e)}"}), 500
   
##//#############################################################################
@files_bp.route('/download-and-zip')
async def download_and_zip_files():
    """
    Endpoint to download, zip, and serve files from S3 based on a common root name.
    """
    root_name = session.get('campaign_id')
    
    # --- FIX ---
    # Get the bucket name from the SESSION
    s3_bucket = session.get('selected_location')
    if not s3_bucket:
        logger.error("❌ Location (bucket name) missing from session for zipping.")
        return jsonify({"error": "Location is missing from session. Please select a location first."}), 400
    # --- END FIX ---
    
    s3_client = current_app.s3_client

    if not s3_client:
        return jsonify({"error": "S3 client not initialized."}), 500
    if not root_name:
        return jsonify({"error": "Campaign ID missing from session. Cannot determine files to zip."}), 400

    logger.info(f"Received request to zip files for campaign '{root_name}' from bucket '{s3_bucket}'")

    all_files, status_code = await list_s3_files(s3_bucket)
    if status_code != 200:
        return jsonify(all_files), status_code 

    files_to_zip = [f for f in all_files if f.startswith(root_name)]

    if not files_to_zip:
        return jsonify({"message": f"No files found starting with '{root_name}' in bucket '{s3_bucket}'."}), 404

    logger.info(f"Found {len(files_to_zip)} files to zip: {files_to_zip}")

    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zf:
        for file_key in files_to_zip:
            try:
                logger.info(f"Downloading file: {file_key}")
                file_content = await download_s3_file(s3_bucket, file_key) 
                zf.writestr(os.path.basename(file_key), file_content)
                logger.info(f"Added {file_key} to zip.")
            except FileNotFoundError as e:
                logger.warning(f"Skipping file '{file_key}': {e}")
            except PermissionError as e:
                logger.error(f"Permission error for file '{file_key}': {e}")
                return jsonify({"error": f"Permission denied for one or more files. {e}"}), 403
            except Exception as e:
                logger.error(f"Error processing file '{file_key}': {e}")
                return jsonify({"error": f"Failed to process file '{file_key}': {e}"}), 500

    zip_buffer.seek(0) 

    zip_filename = f"{root_name}_files.zip"
    logger.info(f"Sending zip file: {zip_filename}")
    return await send_file(
        zip_buffer,
        mimetype='application/zip',
        as_attachment=True,
        attachment_filename=zip_filename
    )
