import os
import io
import zipfile
from quart import Quart, Blueprint, render_template, jsonify, request, Response, session, send_file
from quart import redirect, url_for
import boto3 #  
#import aiobotocore.session # Added aiobotocore
from botocore.exceptions import ClientError
import datetime
import aiohttp
import asyncio
import logging
from database import fetch_all_rows, execute_db_update # Assuming these are properly implemented
from utils.process_routes import mainProcessData # Assuming this is properly implemented
from config import Config # Assuming these are properly implemented
import re


# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Define blueprint
bp = Blueprint('index', __name__)

html_template = "index.html" # Default HTML template for the root route

# S3 Configuration
S3_BUCKET = "urban-heat-island-data" # This is the bucket name used consistently
S3_REGION = "us-west-2"
s3_client = boto3.client("s3", region_name=S3_REGION)

 

# Create the Quart app instance
app = Quart(__name__) # Define app here for standalone testing of this file

# Quart lifecycle hooks to manage async_s3_client
""" 
@app.before_serving
async def create_s3_client_on_startup():
    global async_s3_client
    session_s3 = aiobotocore.session.get_session()
    try:
        async_s3_client = session_s3.create_client(
            's3',
            region_name=S3_REGION,
            #aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID'),
            #aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY')
        )
        await async_s3_client.__aenter__()
        logger.info(f"Asynchronous S3 client initialized for region: {S3_REGION}")
    except Exception as e:
        logger.error(f"Error initializing asynchronous S3 client: {e}")
        async_s3_client = None


@app.after_serving
async def close_s3_client_on_shutdown():
    global async_s3_client
    if async_s3_client:
        # Use __aexit__ to properly close the client
        await async_s3_client.__aexit__(None, None, None)
        logger.info("Asynchronous S3 client closed.")
 """
# --- Helper Functions ---

async def list_s3_files(bucket_name):
    """Lists all objects (files) in a given S3 bucket using the asynchronous client."""
    if not  s3_client:
        return {"error": "S3 client not initialized."}, 500

    if not bucket_name:
        return {"error": "S3 bucket name not configured."}, 500

    try:
        # Await the aiobotocore client method directly
        response =   s3_client.list_objects_v2(Bucket=bucket_name)
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
    """Downloads a single file from S3 and returns its content as bytes using the asynchronous client."""
    if not s3_client:
        raise Exception("S3 client not initialized.")
    if not bucket_name:
        raise Exception("S3 bucket name not configured.")

    try:
        # Await the aiobotocore client method
        response =  s3_client.get_object(Bucket=bucket_name, Key=file_key)
        # The 'Body' stream returned by aiobotocore also needs to be awaited for reading
        file_content = await response['Body'].read()
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


##//####### Health check ######################################################
# Called by lambda function once per minute to check if the server is up,
# if not it will be restarted

@bp.route('/health')
def health():
    """Simple health check endpoint."""
    return "OK", 200

##//###########################################################################
# Render main index page
@bp.route('/')
async def index():
    """Renders the main index page."""
    logger.info("Rendering index page")
    return await render_template(html_template)

##//#############################################################################
# List files in S3 bucket
@bp.route('/uploadCSV')
async def uploadCSV():
    """Renders the CSV upload page."""
    logger.info("Rendering uploadCSV page")
    return await render_template("uploadCSV.html")

##//#############################################################################
@bp.route("/get_presigned_url", methods=["POST"])
async def get_presigned_url():
    """
    Generates a pre-signed S3 PUT URL for uploading CSV files.
    Validates the filename format (must end with _NNN.csv).
    """
    try:
        data = await request.get_json()
        filename = data.get("filename")

        # Ensure the  s3_client is initialized
        if not s3_client:
            return jsonify({"error": "S3 client not initialized. Cannot generate presigned URL."}), 500

        # Validate filename ends with _NNN.csv (e.g., mydata_001.csv)
        if not filename or not re.match(r'^.+_\d{3}\.csv$', filename):
            logger.warning(f"Invalid filename format received: {filename}")
            return jsonify({"error": "Filename must end with _NNN.csv where NNN is a 3-digit number."}), 400

        s3_key = filename

        # Generate pre-signed PUT URL valid for 10 minutes (600 seconds)
        # generate_presigned_url is a synchronous method on the boto3 client, but aiobotocore wraps it.
        # It typically doesn't need to be awaited directly like object operations.
        presigned_url = s3_client.generate_presigned_url(
            ClientMethod="put_object",
            Params={
                "Bucket": S3_BUCKET,
                "Key": s3_key,
                "ContentType": "text/csv"
            },
            ExpiresIn=600  # 10 minutes
        )

        public_url = f"https://{S3_BUCKET}.s3.{S3_REGION}.amazonaws.com/{s3_key}"
        logger.info(f"Generated presigned URL for {filename}")

        return jsonify({
            "uploadUrl": presigned_url,
            "publicUrl": public_url
        })

    except Exception as e:
        logger.exception("Error generating pre-signed URL")
        return jsonify({"error": str(e)}), 500

##//#############################################################################
@bp.route('/renderprocessdata')
async def renderprocessdata():
    """
    Renders the data processing configuration page.
    Sets default session values if not already present.
    """
    # Set default values for processing parameters if not already in session
    session.setdefault('start_time_adjustment_minutes', 1.0) # Changed to float for consistency
    session.setdefault('end_time_adjustment_minutes', 1.0)   # Changed to float for consistency
    session.setdefault('cutoff_speed_MPH', 1.0)
    session.setdefault('slope_option', 1)
    session.setdefault('temperature_drift_f', 0.0)
    session.setdefault('min_q', 3)
    session.setdefault('max_q', 97)
    session.setdefault('solid_color', False)

    logger.info("Rendering processdata page")
    campaign_id = session.get('campaign_id')

    # Pass session values as keyword arguments to the template
    return await render_template("renderprocessdata.html", **session)

##//#############################################################################
## Route that will call python code to process the temperature data ####################################

@bp.route('/run_processing', methods=['POST'])
async def run_processing():
    """
    Triggers the data processing for a selected campaign based on provided parameters.
    Updates session with processed results.
    """
    logger.info("📥 Received POST at /run_processing")

    campaign_id = session.get('campaign_id')
    if not campaign_id:
        logger.error("❌ Campaign ID missing from session for processing.")
        return Response("Campaign ID is missing from session. Please select a campaign first.", status=400, content_type='text/plain')

    data = await request.get_json()
    if not data:
        logger.error("❌ No JSON body in request for /run_processing.")
        return Response("No input data provided for processing.", status=400, content_type='text/plain')

    logger.info(f"🧾 Payload received for processing: {data}")

    # Update session with user-provided values
    # Note: 'campaign_id' is already from session, no need to update from payload unless it changes
    session['start_time_adjustment_minutes'] = data.get('start_time_adjustment_minutes', session.get('start_time_adjustment_minutes'))
    session['end_time_adjustment_minutes'] = data.get('end_time_adjustment_minutes', session.get('end_time_adjustment_minutes'))


    session['cutoff_speed_MPH'] = data.get('cutoff_speed_MPH', session.get('cutoff_speed_MPH'))
    session['slope_option'] = data.get('slope_option', session.get('slope_option'))
    session['temperature_drift_f'] = data.get('temperature_drift_f', session.get('temperature_drift_f'))
    session['min_q'] = data.get('color_table_min_quantile', session.get('min_q'))
    session['max_q'] = data.get('color_table_max_quantile', session.get('max_q'))
    session['solid_color'] = data.get('solid_color', session.get('solid_color'))

    try:
        root_name = data.get('process_id', campaign_id) # Use process_id if provided, else campaign_id

        # Helper functions for parsing values with error handling
        def parse_float(field, default):
            try:
                return float(data.get(field, default))
            except (ValueError, TypeError):
                raise ValueError(f"'{field}' must be a number.")

        def parse_int(field, default):
            try:
                return int(data.get(field, default))
            except (ValueError, TypeError):
                raise ValueError(f"'{field}' must be an integer.")

        start_time_adjustment_minutes = parse_float('start_time_adjustment_minutes', session['start_time_adjustment_minutes'])
        end_time_adjustment_minutes = parse_float('end_time_adjustment_minutes', session['end_time_adjustment_minutes'])

        # Corrected field name here as well
        cutoff_speed_MPH = parse_float('cutoff_speed_MPH', session['cutoff_speed_MPH'])

        slope_option = parse_int('slope_option', session['slope_option'])
        temperature_drift_f_input = parse_float('temperature_drift_f', session['temperature_drift_f']) # Renamed to avoid conflict with return value
        color_table_min_quantile = parse_int('color_table_min_quantile', session['min_q'])
        color_table_max_quantile = parse_int('color_table_max_quantile', session['max_q'])
        solid_color = bool(data.get('solid_color', session['solid_color']))

        logger.info(f"⚙️ Calling mainProcessData for campaign {root_name} with parameters: "
                            f"start_time_adj={start_time_adjustment_minutes}, end_time_adj={end_time_adjustment_minutes}, "
                            f"cutoff_speed={cutoff_speed_MPH}, slope_option={slope_option}, "
                            f"temp_drift_input={temperature_drift_f_input}, min_q={color_table_min_quantile}, "
                            f"max_q={color_table_max_quantile}, solid_color={solid_color}")

        # This will process the data and also return the final drift value in deg F/sec
        # Use asyncio.to_thread if mainProcessData is a blocking function
        temperature_drift_f_output, campaign_duration_seconds, maximum_temperature_correction_f,max_corrected_temperature_f,min_corrected_temperature_f = \
            await asyncio.to_thread(mainProcessData,
                                    root_name=root_name,
                                    start_time_adjustment_minutes=start_time_adjustment_minutes,
                                    end_time_adjustment_minutes=end_time_adjustment_minutes,
                                    cutoff_speed_MPH=cutoff_speed_MPH,
                                    slope_option=slope_option,
                                    temperature_drift_f=temperature_drift_f_input,
                                    color_table_min_quantile=color_table_min_quantile,
                                    color_table_max_quantile=color_table_max_quantile,
                                    solid_color=solid_color)

        # Update session with results calculated in mainProcessData
        session['temperature_drift_f'] = round(temperature_drift_f_output, 6)
        session['campaign_duration_minutes'] = round(campaign_duration_seconds / 60, 2)
        session['maximum_temperature_correction_f'] = round(maximum_temperature_correction_f, 3)
        session['max_corrected_temperature_f'] = round(max_corrected_temperature_f,2)
        session['min_corrected_temperature_f'] = round(min_corrected_temperature_f,2)

        logger.info("✅ mainProcessData completed successfully")
        return jsonify({
            "status": "ok",
            "message": "Processing completed successfully",
            "temperature_drift_f": session.get("temperature_drift_f"),
            "campaign_duration_minutes": session.get("campaign_duration_minutes"),
            "maximum_temperature_correction_f": session.get("maximum_temperature_correction_f"),
            "max_corrected_temperature_f": session.get("max_corrected_temperature_f"),
            "min_corrected_temperature_f": session.get("min_corrected_temperature_f")
        })

    except ValueError as ve:
        logger.warning(f"⚠️ Validation error during processing: {ve}")
        return jsonify({"status": "error", "message": str(ve)}), 400
    except Exception as e:
        logger.exception("❌ Error during processing in /run_processing route")
        return jsonify({"status": "error", "message": "An unexpected error occurred during processing.", "details": str(e)}), 500

##//#############################################################################
@bp.route('/map')
async def map_view():
    """Fetches and renders the map HTML from S3 for the selected campaign."""
    campaign_id = session.get('campaign_id')
    logger.info(f"In map_view function: Fetching map for campaign_id: {campaign_id}")

    if not campaign_id:
        logger.error("❌ Campaign ID missing from session for map view.")
        return Response("Campaign ID is missing. Please select a campaign.", status=400, content_type='text/plain')

    if not s3_client:
        return jsonify({"error": "S3 client not initialized. Cannot fetch map."}), 500

    bucket_name = S3_BUCKET
    key = f"{campaign_id}_color_coded_temperature_map.html"

    try:
        obj = await asyncio.to_thread(s3_client.get_object, Bucket=bucket_name, Key=key)
        html_content = obj['Body'].read().decode('utf-8')
        logger.info(f"Fetched {key} from S3 successfully.")
        return Response(html_content, content_type='text/html')
    except s3_client.exceptions.NoSuchKey:
        logger.error(f"Map file '{key}' not found in S3 bucket.")
        return Response(f"Map file for campaign {campaign_id} not found.", status=404, content_type='text/plain')
    except Exception as e:
        logger.error(f"Failed to fetch {key} from S3: {e}")
        return Response(f"Could not load map HTML for campaign {campaign_id}. Details: {e}", status=500, content_type='text/plain')

##//#############################################################################
@bp.route('/temperatureplot')
async def temperature_plot():
    """Fetches and renders the temperature plot HTML from S3 for the selected campaign."""
    campaign_id = session.get('campaign_id')
    logger.info(f"In temperature_plot function: Fetching plot for campaign_id: {campaign_id}")

    if not campaign_id:
        logger.error("❌ Campaign ID missing from session for temperature plot.")
        return Response("Campaign ID is missing. Please select a campaign.", status=400, content_type='text/plain')

    if not  s3_client:
        return jsonify({"error": "S3 client not initialized. Cannot fetch plot."}), 500

    bucket_name = S3_BUCKET
    key = f"{campaign_id}_fig1_corrected_temperature_map_time_window.html"

    try:
        # Use the  s3_client  
        obj =   s3_client.get_object(Bucket=bucket_name, Key=key)
        # Await reading the body content
        html_content = await obj['Body'].read()
        logger.info(f"Fetched {key} from S3 successfully.")
        return Response(html_content.decode('utf-8'), content_type='text/html')
    except ClientError as e:
        error_code = e.response.get("Error", {}).get("Code")
        if error_code == 'NoSuchKey':
            logger.error(f"Plot file '{key}' not found in S3 bucket.")
            return Response(f"Plot file for campaign {campaign_id} not found.", status=404, content_type='text/plain')
        else:
            logger.error(f"S3 client error fetching plot {key}: {e}")
            return Response(f"S3 client error fetching plot for campaign {campaign_id}. Details: {e}", status=500, content_type='text/plain')
    except Exception as e:
        logger.error(f"Failed to fetch {key} from S3: {e}")
        return Response(f"Could not load temperature plot HTML for campaign {campaign_id}. Details: {e}", status=500, content_type='text/plain')

##//#############################################################################
@bp.route('/get_campaign_locations', methods=['POST'])
async def get_campaign_locations():
    """
    Fetches campaign metadata from the database.
    Can filter campaigns based on the 'show_hidden' flag received from the frontend.
    """
    logger.info(f"Fetching rows from mobile_metadata...")

    data = await request.get_json() # Correct for Quart
    show_hidden_campaigns = data.get('show_hidden', False)

    # SQL query includes 'hidden' column and conditionally filters based on 'show_hidden_campaigns'
    sql_query = "SELECT campaign_id, campaign_title, owners, run_date, hidden FROM mobile_metadata"
    if not show_hidden_campaigns:
        sql_query += " WHERE hidden = 0"
    sql_query += " ORDER BY run_date DESC"


    # Execute blocking database call in a separate thread for Quart
    rows = await asyncio.to_thread(fetch_all_rows, sql_query)
    logger.info(f"✅ Number of rows fetched: {len(rows)}")
    for row in rows:
        logger.debug(f"Fetched row: {row}") # Use debug for individual rows

    rows_serializable = []
    for row in rows:
        processed_row = {}
        for key, value in row.items():
            if isinstance(value, (datetime.date, datetime.datetime)):
                processed_row[key] = value.isoformat()
            elif key == 'hidden':
                # Convert integer (0/1) from DB to boolean for frontend
                processed_row[key] = bool(value)
            else:
                processed_row[key] = value
        rows_serializable.append(processed_row)

    selected_campaign_id = session.get('campaign_id')

    return jsonify({
        "campaigns": rows_serializable,
        "selected": selected_campaign_id
    })

##//#############################################################################
@bp.route('/update_campaign', methods=['POST'])
async def update_campaign():
    """
    Updates specific fields for a campaign in the 'mobile_metadata' table,
    including 'campaign_title', 'owners', 'run_date', and 'hidden' status.
    """
    try:
        campaign_data = await request.get_json() # Correct for Quart
        logger.info(f"🔵 Received single campaign update: {campaign_data}")

        campaign_id = campaign_data.get('campaign_id')

        # Use campaign_title instead of description to match DB
        campaign_title = campaign_data.get('campaign_title')
        owners = campaign_data.get('owners')

        # Use run_date_str instead of date_str to match DB
        run_date_str = campaign_data.get('run_date')

        # New field: hidden status (boolean from frontend)
        hidden = campaign_data.get('hidden')

        if not campaign_id:
            return jsonify({"error": "Missing campaign_id"}), 400

        # Validate run_date format if provided
        if run_date_str:
            try:
                datetime.datetime.strptime(run_date_str, '%Y-%m-%d')
            except ValueError:
                return jsonify({"error": f"Invalid run_date format: {run_date_str}. Use YYYY-MM-DD."}), 400
        else:
            run_date_str = None # Allow None if no date is provided

        # Convert boolean 'hidden' to integer (0 or 1) for database storage
        hidden_int = 1 if hidden else 0

        # Update SQL to 'mobile_metadata' table and corrected column names
        sql = """
            UPDATE mobile_metadata
            SET campaign_title = %s, owners = %s, run_date = %s, hidden = %s
            WHERE campaign_id = %s
        """
        # Parameters order must match the SQL query's SET clause order
        params = (campaign_title, owners, run_date_str, hidden_int, campaign_id)
        logger.info(f"🔵 Executing SQL: {sql} with params: {params}")

        # Execute blocking database update in a separate thread for Quart
        success = await asyncio.to_thread(execute_db_update, sql, params)

        if success:
            logger.info(f"✅ Successfully updated campaign {campaign_id}")
            return jsonify({"message": f"Campaign {campaign_id} updated successfully."}), 200
        else:
            logger.warning(f"⚠️ Update function returned false for {campaign_id}. Check database operation.")
            return jsonify({"error": "Database update failed."}), 500

    except Exception as e:
        logger.exception("❌ Exception while updating campaign in /update_campaign route")
        return jsonify({"error": "An error occurred", "details": str(e)}), 500


##//#############################################################################
@bp.route('/update_hidden', methods=['POST'])
async def update_hidden():
    """
    Updates only the 'hidden' status for a campaign in the 'mobile_metadata' table.
    """
    try:
        campaign_data = await request.get_json() # Correct for Quart
        logger.info(f"🔵 Received hidden status update: {campaign_data}")

        campaign_id = campaign_data.get('campaign_id')
        hidden = campaign_data.get('hidden') # This should be a boolean from the frontend

        if not campaign_id:
            return jsonify({"error": "Missing campaign_id"}), 400

        # Ensure 'hidden' is explicitly provided and is a boolean
        if hidden is None or not isinstance(hidden, bool):
            return jsonify({"error": "Missing or invalid 'hidden' status. Must be a boolean."}), 400

        # Convert boolean 'hidden' to integer (0 or 1) for database storage
        hidden_int = 1 if hidden else 0

        sql = """
            UPDATE mobile_metadata
            SET hidden = %s
            WHERE campaign_id = %s
        """
        params = (hidden_int, campaign_id)
        logger.info(f"🔵 Executing SQL for hidden status update: {sql} with params: {params}")

        success = await asyncio.to_thread(execute_db_update, sql, params)

        if success:
            logger.info(f"✅ Successfully updated hidden status for campaign {campaign_id} to {hidden_int}")
            return jsonify({"message": f"Campaign {campaign_id} hidden status updated successfully."}), 200
        else:
            logger.warning(f"⚠️ Update function returned false for hidden status update of {campaign_id}. Check database operation.")
            return jsonify({"error": "Database update failed for hidden status."}), 500

    except Exception as e:
        logger.exception("❌ Exception while updating hidden status in /update_hidden route")
        return jsonify({"error": "An error occurred", "details": str(e)}), 500

##//#############################################################################
@bp.route('/registration')
async def registration():
    """Renders the registration page."""
    logger.info("Registration route hit")

    return await render_template("newRegistration.html")
    #return await render_template("registration.html")

##//#############################################################################
@bp.route('/metadata')
async def metadata():
    """Renders the metadata page."""
    logger.info("Metadata route hit")
    return await render_template("metadata.html")

##//#############################################################################
@bp.route('/arcgis')
async def arcgis():
    """Renders the ArcGIS integration page."""
    logger.info("arcgis route hit")
    return await render_template("arcgis.html")

##//#############################################################################
@bp.route('/get_metadata', methods=['POST'])
async def get_metadata(): # Changed to async for consistency with Quart and potential blocking DB
    """
    Fetches detailed metadata for the selected campaign ID from mobile_metadata.
    """
    campaign_id = session.get("campaign_id")
    if not campaign_id:
        logger.warning("No campaign_id in session for get_metadata.")
        return jsonify({"error": "Campaign ID missing from session."}), 400

    query = "SELECT * FROM mobile_metadata WHERE campaign_id = %s"
    # Execute blocking database call in a separate thread for Quart
    rows = await asyncio.to_thread(fetch_all_rows, query, (campaign_id,))
    if not rows:
        logger.warning(f"No metadata found for campaign_id: {campaign_id}")
        return jsonify({"error": f"No metadata found for campaign ID: {campaign_id}"}), 404

    result = {}
    for key, value in rows[0].items():
        if isinstance(value, (datetime.datetime, datetime.date)):
            result[key] = value.isoformat()
        elif key == 'hidden':
            result[key] = bool(value) # Convert to boolean
        else:
            result[key] = value

    logger.info(f"Fetched metadata for campaign_id: {campaign_id}")
    return jsonify(result)

##//#############################################################################
@bp.route('/update_metadata', methods=['POST'])
async def update_metadata():
    """
    Updates editable metadata fields for a campaign in the 'mobile_metadata' table.
    """
    campaign_id = session.get("campaign_id")
    if not campaign_id:
        logger.warning("No campaign_id in session for update_metadata.")
        return jsonify({"error": "Campaign ID missing from session."}), 400

    form = await request.form # Correct for Quart
    # Ensure these fields match your database columns and frontend form names
    editable_fields = ["campaign_title", "campaign_location", "short_description", "owners", "notes"]

    updates = []
    values = []
    for field in editable_fields:
        if field in form:
            updates.append(f"{field} = %s")
            values.append(form.get(field))

    if not updates:
        logger.warning("No data to update received for metadata.")
        return "No data to update.", 400

    values.append(campaign_id)
    # Note: updated_at is automatically handled by CURRENT_TIMESTAMP
    query = f"""
        UPDATE mobile_metadata
        SET {", ".join(updates)},
            updated_at = CURRENT_TIMESTAMP
        WHERE campaign_id = %s
    """

    try:
        # Execute blocking database update in a separate thread for Quart
        await asyncio.to_thread(execute_db_update, query, tuple(values))
        logger.info(f"Successfully updated metadata for campaign_id: {campaign_id}")
        return redirect("/metadata") # Redirect after successful update
    except Exception as e:
        logger.exception(f"Error updating metadata for campaign_id: {campaign_id}")
        return f"Error updating metadata: {str(e)}", 500

##//#############################################################################
@bp.route('/submit_registration', methods=['POST'])
async def submit_registration():
    """
    Handles the submission of a selected campaign ID from the main selection form.
    Stores the selected campaign ID in the session and redirects to the registration page.
    """
    logger.info("📥 /submit_registration route accessed")
    form_data = await request.form # Correct for Quart
    logger.info(f"Form data received: {form_data}")
    campaign_id = form_data.get('campaign_id')

    if campaign_id:
        session['campaign_id'] = campaign_id
        logger.info(f"Stored campaign_id in session: {campaign_id}")
        return redirect('/registration')
    else:
        logger.warning("No campaign_id received in /submit_registration.")
        return jsonify({"error": "No campaign ID provided for registration."}), 400

##//#############################################################################
@bp.route('/submit_metadata', methods=['POST'])
async def submit_metadata():
    """
    Handles the submission of metadata. This route currently only logs and returns a message.
    Extend this to save metadata or trigger further actions.
    """
    logger.info("📥 /submit_metadata route accessed")
    form_data = await request.form # Correct for Quart
    campaign_id = session.get('campaign_id')
    logger.info(f"Received metadata for campaign {campaign_id}: {form_data}")
    return "Thank you for entering metadata!"

##//#############################################################################
@bp.route('/download-and-zip')
async def download_and_zip_files():
    root_name = request.args.get('root_name')

    if not S3_BUCKET:
        return jsonify({"error": "S3_BUCKET_NAME environment variable not set."}), 500
    if not s3_client:
        return jsonify({"error": "S3 client not initialized. Check AWS configuration."}), 500
    if not root_name:
        return jsonify({"error": "Missing 'root_name' query parameter."}), 400

    try:
        response = await asyncio.to_thread(s3_client.list_objects_v2, Bucket=S3_BUCKET_NAME)
        all_files = [obj['Key'] for obj in response.get('Contents', [])]
        files_to_zip = [f for f in all_files if f.startswith(root_name)]

        if not files_to_zip:
            return jsonify({"message": f"No files found starting with '{root_name}' in bucket '{S3_BUCKET_NAME}'."}), 404

        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zf:
            for file_key in files_to_zip:
                try:
                    obj = await asyncio.to_thread(s3_client.get_object, Bucket=S3_BUCKET_NAME, Key=file_key)
                    file_content = obj['Body'].read()
                    zf.writestr(os.path.basename(file_key), file_content)
                except Exception as e:
                    logger.error(f"Error processing file '{file_key}': {e}")
                    return jsonify({"error": f"Failed to process file '{file_key}': {e}"}), 500

        zip_buffer.seek(0)
        zip_filename = f"{root_name}_files.zip"
        return await send_file(
            zip_buffer,
            mimetype='application/zip',
            as_attachment=True,
            download_name=zip_filename
        )

    except Exception as e:
        logger.error(f"Error listing or zipping files: {e}")
        return jsonify({"error": str(e)}), 500

    root_name = session.get('campaign_id')

    if not S3_BUCKET:
        return jsonify({"error": "S3_BUCKET environment variable not set."}), 500
    if not  s3_client:  
        return jsonify({"error": "S3 client not initialized. Check AWS configuration."}), 500
    if not root_name:
        # Updated error message to reflect root_name comes from session
        return jsonify({"error": "Campaign ID missing from session. Cannot determine files to zip."}), 400

    logger.info(f"Received request to zip files with root_name (campaign_id): {root_name}")


    # List all files in the bucket using the async helper function
    all_files, status_code = await list_s3_files(S3_BUCKET)
    if status_code != 200:
        return jsonify(all_files), status_code # Return error from list_s3_files

    # Filter files based on common root name
    files_to_zip = [f for f in all_files if f.startswith(root_name)]

    if not files_to_zip:
        return jsonify({"message": f"No files found starting with '{root_name}' in bucket '{S3_BUCKET}'."}), 404

    logger.info(f"Found {len(files_to_zip)} files to zip: {files_to_zip}")

    # Create an in-memory zip file
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zf:
        for file_key in files_to_zip:
            try:
                logger.info(f"Downloading file: {file_key}")
                # Download file using the async helper function
                file_content = await download_s3_file(S3_BUCKET, file_key)
                # Add file to zip archive. Use os.path.basename to avoid creating
                # nested directories in the zip if file_key contains paths.
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

    zip_buffer.seek(0) # Rewind the buffer to the beginning

    # Make the zip file downloadable
    zip_filename = f"{root_name}_files.zip"
    logger.info(f"Sending zip file: {zip_filename}")
    return await send_file(
        zip_buffer,
        mimetype='application/zip',
        as_attachment=True,
        download_name=zip_filename
    )

# Register the blueprint with the main app if this file is intended to be run directly
# In a larger project, this would typically be done in your main __init__.py or app.py
app.register_blueprint(bp)

if __name__ == '__main__':
    # Ensure S3_BUCKET is set before running if not hardcoded
    # In this specific code, S3_BUCKET is hardcoded, but if it were from env, this check is vital.
    # For local testing, you might still need AWS credentials.
    if not os.environ.get('AWS_ACCESS_KEY_ID') or not os.environ.get('AWS_SECRET_ACCESS_KEY'):
        print("Warning: AWS_ACCESS_KEY_ID or AWS_SECRET_ACCESS_KEY environment variables are not set.")
        print("Please set them for S3 operations to work correctly, e.g.:")
        print("export AWS_ACCESS_KEY_ID='YOUR_ACCESS_KEY_ID'")
        print("export AWS_SECRET_ACCESS_KEY='YOUR_SECRET_ACCESS_KEY'")
        # It's better to allow the app to run and let the S3 client initialization fail
        # gracefully, rather than exiting immediately, as some routes might not need S3.
        # However, for S3-dependent apps, exiting early might be desirable.
        # For now, let's just warn. The S3 client init will fail, and routes will handle it.
        pass

    # Quart requires a secret key for session management
    app.secret_key = os.environ.get('QUART_SECRET_KEY', 'a_super_secret_key_for_dev')
    if app.secret_key == 'a_super_secret_key_for_dev':
        logger.warning("QUART_SECRET_KEY not set in environment. Using a default key. Set a strong, unique key in production!")

    app.run(debug=True, port=5000)

# This BACKEND routefile is part of a Quart application that handles various routes for
# processing and serving data related to temperature campaigns, including
# uploading CSV files, processing temperature data, and serving HTML pages
# for maps and plots.
# It also includes functionality for zipping and downloading files from an S3 bucket.
# The application uses AWS S3 for file storage and retrieval, and it provides
# endpoints for health checks, metadata management, and campaign registration.