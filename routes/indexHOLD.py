import logging
import boto3
import plotly.graph_objs as go
import plotly.io as pio
from quart import Quart, Blueprint, render_template, jsonify, request, Response, session
from quart import redirect, url_for
import datetime
import aiohttp
import asyncio
from database import fetch_all_rows, execute_db_update
from utils.process_routes import mainProcessData
from config import Config
import re

bp = Blueprint('index', __name__)
logger = logging.getLogger(__name__)
html_template = "index.html"
 
S3_BUCKET = "urban-heat-island-data"
S3_REGION = "us-west-2"
s3_client = boto3.client("s3", region_name=S3_REGION)

##//####### Health check ######################################################
@bp.route('/health')
def health():
    return "OK", 200

##//###########################################################################
@bp.route('/')
async def index():
    logger.info("Rendering index page")
    return await render_template(html_template)

##//#############################################################################
@bp.route('/uploadCSV')
async def uploadCSV():
    logger.info("Rendering uploadCSV page")
    return await render_template("uploadCSV.html")


##//#############################################################################
@bp.route("/get_presigned_url", methods=["POST"])
async def get_presigned_url():
    try:
        data = await request.get_json()
        filename = data.get("filename")

        # ✅ Validate filename ends with _NNN.csv
        if not filename or not re.match(r'^.+_\d{3}\.csv$', filename):

            return jsonify({"error": "Filename must end with -NNN.csv where NNN is a 3-digit number."}), 400

        s3_key = filename

        # ✅ Generate pre-signed PUT URL valid for 10 minutes
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

        #logger.info(f"Generated presigned URL:\n{presigned_url}")

        return jsonify({
            "uploadUrl": presigned_url,
            "publicUrl": public_url
        })

    except Exception as e:
        return jsonify({"error": str(e)}), 500

##//#############################################################################
@bp.route('/renderprocessdata')
async def renderprocessdata():
     # Set default values once
    session.setdefault('start_time_adjustment_minutes', '1.0')
    session.setdefault('end_time_adjustment_minutes', '1.0')
    session.setdefault('cutoff_speed_MPH', 1.0)
    session.setdefault('slope_option',1)    
    session.setdefault('temperature_drift_f', 0.0)
    session.setdefault('min_q', 3)
    session.setdefault('max_q', 97)
    session.setdefault('solid_color', False)
    logger.info("Rendering processdata page")
    campaign_id = session.get('campaign_id')
    return await render_template("renderprocessdata.html", **session)

##//#############################################################################
@bp.route('/run_processing', methods=['POST'])
async def run_processing():
    logger.info("📥 Received POST at /run_processing")

    campaign_id = session.get('campaign_id')
    if not campaign_id:
        logger.error("❌ Campaign ID missing from session")
        return Response("Campaign ID is missing.", status=400, content_type='text/plain')

    data = await request.get_json()
    if not data:
        logger.error("❌ No JSON body in request")
        return Response("No input data provided", status=400, content_type='text/plain')

    logger.info(f"🧾 Payload received: {data}")

# Update session with user-provided values

    session['campaign_id'] = campaign_id

    session['start_time_adjustment_minutes'] = data.get('start_time_adjustment_minutes', '1.0')
    session['end_time_adjustment_minutes'] = data.get('end_time_adjustment_minutes', '1.0')
    session['cutoff_speed_MPH'] = data.get('cuttoff_speed_MPH', 1.0)
    session['slope_option'] = data.get('slope_option', 1)
    session['temperature_drift_f'] = data.get('temperature_drift_f', 0.0)
    session['min_q'] = data.get('color_table_min_quantile', 5)
    session['max_q'] = data.get('color_table_max_quantile', 95)
    session['solid_color'] = data.get('solid_color', False)
    try:
        root_name = data.get('process_id', campaign_id)

        
        def parse_float(field, default):
            try:
                return float(data.get(field, default))
            except (ValueError, TypeError):
                raise ValueError(f"{field} must be a number")

        def parse_int(field, default):
            try:
                return int(data.get(field, default))
            except (ValueError, TypeError):
                raise ValueError(f"{field} must be an integer")
            

        start_time_adjustment_minutes = parse_float('start_time_adjustment_minutes', 1.0)
        end_time_adjustment_minutes = parse_float('end_time_adjustment_minutes', 1.0)   
        cuttoff_speed_MPH = parse_float('cuttoff_speed_MPH', 1.0)
        slope_option = parse_int('slope_option', 1)
        temperature_drift_f = parse_float('temperature_drift_f', 0.0)
        color_table_min_quantile = parse_int('color_table_min_quantile', 5)
        color_table_max_quantile = parse_int('color_table_max_quantile', 95)
        solid_color  = bool(data.get('solid_color', False))

        logger.info(f"⚙️ Calling mainProcessData for campaign {root_name} to process all input data")

        temperature_drift_f,campaign_duration_seconds,maximum_temperature_correction_f  = mainProcessData(   # This will process the data and also return the final drift value in deg F/sec
            root_name=root_name,  
            start_time_adjustment_minutes=start_time_adjustment_minutes,
            end_time_adjustment_minutes=end_time_adjustment_minutes,
            cuttoff_speed_MPH=cuttoff_speed_MPH,
            slope_option=slope_option,
            temperature_drift_f=temperature_drift_f,
            color_table_min_quantile=color_table_min_quantile,
            color_table_max_quantile=color_table_max_quantile,
            solid_color =solid_color 
        )
        session['temperature_drift_f'] = round(temperature_drift_f, 6)
        session['campaign_duration_minutes'] = round(campaign_duration_seconds/60,2)
        session['maximum_temperature_correction_f'] = round(maximum_temperature_correction_f, 3)

        logger.info("✅ mainProcessData completed successfully")
        return jsonify({
            "status": "ok",
            "message": "Processing completed successfully",
            "temperature_drift_f": session.get("temperature_drift_f"),
            "campaign_duration_minutes": session.get("campaign_duration_minutes"),
            "maximum_temperature_correction_f": session.get("maximum_temperature_correction_f")
        })

    except ValueError as ve:
        logger.warning(f"⚠️ Validation error: {ve}")
        return jsonify({"status": "error", "message": str(ve)}), 400
    except Exception as e:
        logger.exception("❌ Error during processing")

        

##//#############################################################################
@bp.route('/map')
async def map_view():
    campaign_id = session.get('campaign_id')
    logger.info(f"In map_view function: Fetching map for campaign_id: {campaign_id}")

    if not campaign_id:
        return Response("Campaign ID is missing.", status=400, content_type='text/plain')

    bucket_name = "urban-heat-island-data"
    key = f"{campaign_id}_color_coded_temperature_map.html"

    s3 = boto3.client('s3')

    try:
        obj = s3.get_object(Bucket=bucket_name, Key=key)
        html_content = obj['Body'].read().decode('utf-8')
        #logger.info(f"Fetched {key} from S3")
        return Response(html_content, content_type='text/html')
    except Exception as e:
        logger.error(f"Failed to fetch {key} from S3: {e}")
        return Response("Could not load map HTML.", status=500, content_type='text/plain')

##//#############################################################################
@bp.route('/temperatureplot')
async def temperature_plot():
    campaign_id = session.get('campaign_id')
    #logger.info(f"In temperature_plot function: Fetching plot for campaign_id: {campaign_id}")

    bucket_name = "urban-heat-island-data"

    key = f"{campaign_id}_fig1_corrected_temperature_map_time_window.html"

    s3 = boto3.client('s3')

    try:
        obj = s3.get_object(Bucket=bucket_name, Key=key)
        html_content = obj['Body'].read().decode('utf-8')
        #logger.info(f"Fetched {key} from S3")
        return Response(html_content, content_type='text/html')
    except Exception as e:
        logger.error(f"Failed to fetch {key} from S3: {e}")
        return Response("Could not load map HTML.", status=500, content_type='text/plain')

##//#############################################################################
@bp.route('/get_campaign_locations', methods=['POST'])
async def get_campaign_locations():
    logger.info(f"Fetching all rows in mobile_metadata")

    data = request.get_json() # Get the JSON data from the request body
    show_hidden_campaigns = data.get('show_hidden', False) # Default to False if not provided

    sql_query = "SELECT campaign_id, campaign_title, owners, run_date, hidden FROM mobile_metadata" # Added 'hidden' column
    if not show_hidden_campaigns:
        sql_query += " WHERE hidden = 0" # Only add the WHERE clause if we don't want to show hidden
    sql_query += " ORDER BY run_date"

    rows = fetch_all_rows(sql_query)
    logger.info(f"✅ Number of rows fetched: {len(rows)}")
    for row in rows:
        logger.info(f" {row}")

    rows_serializable = []
    for row in rows:
        processed_row = {}
        for key, value in row.items():
            if isinstance(value, (datetime.date, datetime.datetime)):
                processed_row[key] = value.isoformat()
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
    try:
        campaign_data = await request.get_json()
        logger.info(f"🔵 Received single campaign update: {campaign_data}")

        campaign_id = campaign_data.get('campaign_id')
        description = campaign_data.get('description')
        owners = campaign_data.get('owners')
        date_str = campaign_data.get('date')

        if not campaign_id:
            return jsonify({"error": "Missing campaign_id"}), 400

        try:
            if date_str:
                datetime.datetime.strptime(date_str, '%Y-%m-%d')
            else:
                date_str = None
        except ValueError:
            return jsonify({"error": f"Invalid date format: {date_str}. Use YYYY-MM-DD."}), 400

        sql = """
            UPDATE campaign_locations
            SET description = %s, owners = %s, date = %s
            WHERE campaign_id = %s
        """
        params = (description, owners, date_str, campaign_id)

        success = execute_db_update(sql, params)

        if success:
            logger.info(f"✅ Successfully updated campaign {campaign_id}")
            return jsonify({"message": f"Campaign {campaign_id} updated successfully."}), 200
        else:
            logger.warning(f"⚠️ Update function returned false for {campaign_id}")
            return jsonify({"error": "Database update failed."}), 500

    except Exception as e:
        logger.exception("❌ Exception while updating campaign")
        return jsonify({"error": "An error occurred", "details": str(e)}), 500

##//#############################################################################
@bp.route('/registration')
async def registration():
    logger.info("Registration route hit")
    return await render_template("registration.html")

##//#############################################################################
@bp.route('/metadata')
async def metadata():
    logger.info("Metadata route hit")
    return await render_template("metadata.html")

##//#############################################################################
@bp.route('/arcgis')
async def arcgis():
    logger.info("arcgis route hit")
    return await render_template("arcgis.html")

##//#############################################################################
@bp.route('/get_metadata', methods=['POST'])
def get_metadata():
    campaign_id = session.get("campaign_id")
    if not campaign_id:
        return jsonify({}), 400

    query = "SELECT * FROM mobile_metadata WHERE campaign_id = %s"
    rows = fetch_all_rows(query, (campaign_id,))
    if not rows:
        return jsonify({}), 404

    result = {}
    for key, value in rows[0].items():
        result[key] = value.isoformat() if isinstance(value, (datetime.datetime, datetime.date)) else value

    return jsonify(result)

##//#############################################################################
@bp.route('/update_metadata', methods=['POST'])
async def update_metadata():
    campaign_id = session.get("campaign_id")
    if not campaign_id:
        return jsonify({}), 400

    form = await request.form
    editable_fields = ["campaign_title", "campaign_location", "short_description", "owners", "notes"]

    updates = []
    values = []
    for field in editable_fields:
        if field in form:
            updates.append(f"{field} = %s")
            values.append(form.get(field))

    if not updates:
        return "No data to update.", 400

    values.append(campaign_id)
    query = f"""
        UPDATE mobile_metadata
        SET {", ".join(updates)},
            updated_at = CURRENT_TIMESTAMP
        WHERE campaign_id = %s
    """

    try:
        await asyncio.to_thread(execute_db_update, query, tuple(values))
        return redirect("/metadata")
    except Exception as e:
        return f"Error updating metadata: {str(e)}", 500

##//#############################################################################
@bp.route('/submit_registration', methods=['POST'])
async def submit_registration():
    logger.info("📥 /submit_registration route accessed")
    form_data = await request.form
    logger.info(f"Form data received: {form_data}")
    campaign_id = form_data.get('campaign_id')
    session['campaign_id'] = campaign_id
    logger.info(f"Stored campaign_id in session variables: {campaign_id}")
    return redirect('/registration')

##//#############################################################################
@bp.route('/submit_metadata', methods=['POST'])
async def submit_metadata():
    logger.info("📥 /submit_metadata route accessed")
    form_data = await request.form
    campaign_id = session.get('campaign_id')
    logger.info("Received metadata")
    return "Thank you for entering metadata!"

##//#############################################################################
@bp.route('/download-and-zip', methods=['GET'])

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

    bp.logger.info(f"Received request to zip files with root_name: {root_name}")

    # 1. List all files in the bucket
    all_files, status_code = await list_s3_files(S3_BUCKET_NAME)
    if status_code != 200:
        return jsonify(all_files), status_code # Return error from list_s3_files

    # 2. Filter files based on common root name
    files_to_zip = [f for f in all_files if f.startswith(root_name)]

    if not files_to_zip:
        return jsonify({"message": f"No files found starting with '{root_name}' in bucket '{S3_BUCKET_NAME}'."}), 404

    bp.logger.info(f"Found {len(files_to_zip)} files to zip: {files_to_zip}")

    # 3. Create a in-memory zip file
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zf:
        for file_key in files_to_zip:
            try:
                bp.logger.info(f"Downloading file: {file_key}")
                file_content = await download_s3_file(S3_BUCKET_NAME, file_key)
                # Add file to zip archive. Use os.path.basename to avoid creating
                # nested directories in the zip if file_key contains paths.
                zf.writestr(os.path.basename(file_key), file_content)
                bp.logger.info(f"Added {file_key} to zip.")
            except FileNotFoundError as e:
                bp.logger.warning(f"Skipping file '{file_key}': {e}")
            except PermissionError as e:
                bp.logger.error(f"Permission error for file '{file_key}': {e}")
                return jsonify({"error": f"Permission denied for one or more files. {e}"}), 403
            except Exception as e:
                bp.logger.error(f"Error processing file '{file_key}': {e}")
                return jsonify({"error": f"Failed to process file '{file_key}': {e}"}), 500

    zip_buffer.seek(0) # Rewind the buffer to the beginning

    # 4. Make the zip file downloadable
    zip_filename = f"{root_name}_files.zip"
    bp.logger.info(f"Sending zip file: {zip_filename}")
    return await send_file(
        zip_buffer,
        mimetype='application/zip',
        as_attachment=True,
        download_name=zip_filename
    )

