import os
import io
import zipfile
from quart import Quart, request, send_file, jsonify
import boto3
from botocore.exceptions import ClientError

# Initialize Quart app
app = Quart(__name__)

# --- Configuration ---
# It's highly recommended to use environment variables for sensitive information
# like AWS credentials and bucket names, especially in production.
# For local testing, you can set these in your shell:
# export AWS_ACCESS_KEY_ID='YOUR_ACCESS_KEY_ID'
# export AWS_SECRET_ACCESS_KEY='YOUR_SECRET_ACCESS_KEY'
# export AWS_REGION='your-aws-region' # e.g., 'us-east-1'
# export S3_BUCKET_NAME='your-s3-bucket-name'

S3_BUCKET_NAME = os.environ.get('S3_BUCKET_NAME')
AWS_REGION = os.environ.get('AWS_REGION', 'us-east-1') # Default to us-east-1 if not set

# Initialize S3 client
try:
    s3_client = boto3.client(
        's3',
        region_name=AWS_REGION,
        aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY')
    )
    print(f"S3 client initialized for region: {AWS_REGION}")
except Exception as e:
    print(f"Error initializing S3 client: {e}")
    s3_client = None # Set to None to handle cases where client initialization fails

# --- Helper Functions ---

async def list_s3_files(bucket_name):
    """Lists all objects (files) in a given S3 bucket."""
    if not s3_client:
        return {"error": "S3 client not initialized."}, 500

    if not bucket_name:
        return {"error": "S3 bucket name not configured."}, 500

    try:
        response = await app.loop.run_in_executor(
            None, lambda: s3_client.list_objects_v2(Bucket=bucket_name)
        )
        files = []
        for obj in response.get('Contents', []):
            files.append(obj['Key']) # 'Key' is the file name/path in S3
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
    if not s3_client:
        raise Exception("S3 client not initialized.")
    if not bucket_name:
        raise Exception("S3 bucket name not configured.")

    try:
        response = await app.loop.run_in_executor(
            None, lambda: s3_client.get_object(Bucket=bucket_name, Key=file_key)
        )
        return await app.loop.run_in_executor(None, lambda: response['Body'].read())
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

# --- API Endpoints ---

@app.route('/')
async def index():
    """Simple health check or welcome message."""
    return "Quart S3 Backend is running!"

@app.route('/list-files')
async def get_s3_files():
    """
    Endpoint to list all files in the configured S3 bucket.
    Example: GET /list-files
    """
    files, status_code = await list_s3_files(S3_BUCKET_NAME)
    if status_code != 200:
        return jsonify(files), status_code
    return jsonify({"files": files}), 200

@app.route('/download-and-zip', methods=['GET'])
async def download_and_zip_files():
    """
    Endpoint to download, zip, and serve files from S3 based on a common root name.
    Query Parameters:
        root_name (string, required): The common root name to filter files by.
                                      e.g., if root_name='report', it will match 'report_jan.pdf', 'report_feb.csv', etc.
    Example: GET /download-and-zip?root_name=my_document
    """
    root_name = request.args.get('root_name')

    if not S3_BUCKET_NAME:
        return jsonify({"error": "S3_BUCKET_NAME environment variable not set."}), 500
    if not s3_client:
        return jsonify({"error": "S3 client not initialized. Check AWS configuration."}), 500
    if not root_name:
        return jsonify({"error": "Missing 'root_name' query parameter."}), 400

    app.logger.info(f"Received request to zip files with root_name: {root_name}")

    # 1. List all files in the bucket
    all_files, status_code = await list_s3_files(S3_BUCKET_NAME)
    if status_code != 200:
        return jsonify(all_files), status_code # Return error from list_s3_files

    # 2. Filter files based on common root name
    files_to_zip = [f for f in all_files if f.startswith(root_name)]

    if not files_to_zip:
        return jsonify({"message": f"No files found starting with '{root_name}' in bucket '{S3_BUCKET_NAME}'."}), 404

    app.logger.info(f"Found {len(files_to_zip)} files to zip: {files_to_zip}")

    # 3. Create a in-memory zip file
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zf:
        for file_key in files_to_zip:
            try:
                app.logger.info(f"Downloading file: {file_key}")
                file_content = await download_s3_file(S3_BUCKET_NAME, file_key)
                # Add file to zip archive. Use os.path.basename to avoid creating
                # nested directories in the zip if file_key contains paths.
                zf.writestr(os.path.basename(file_key), file_content)
                app.logger.info(f"Added {file_key} to zip.")
            except FileNotFoundError as e:
                app.logger.warning(f"Skipping file '{file_key}': {e}")
            except PermissionError as e:
                app.logger.error(f"Permission error for file '{file_key}': {e}")
                return jsonify({"error": f"Permission denied for one or more files. {e}"}), 403
            except Exception as e:
                app.logger.error(f"Error processing file '{file_key}': {e}")
                return jsonify({"error": f"Failed to process file '{file_key}': {e}"}), 500

    zip_buffer.seek(0) # Rewind the buffer to the beginning

    # 4. Make the zip file downloadable
    zip_filename = f"{root_name}_files.zip"
    app.logger.info(f"Sending zip file: {zip_filename}")
    return await send_file(
        zip_buffer,
        mimetype='application/zip',
        as_attachment=True,
        download_name=zip_filename
    )

if __name__ == '__main__':
    # Ensure S3_BUCKET_NAME is set before running
    if not S3_BUCKET_NAME:
        print("Error: S3_BUCKET_NAME environment variable is not set.")
        print("Please set it before running the application, e.g.:")
        print("export S3_BUCKET_NAME='your-s3-bucket-name'")
        print("export AWS_ACCESS_KEY_ID='YOUR_ACCESS_KEY_ID'")
        print("export AWS_SECRET_ACCESS_KEY='YOUR_SECRET_ACCESS_KEY'")
        print("export AWS_REGION='your-aws-region'")
        exit(1)

    app.run(debug=True, port=5000) # Run in debug mode for development
