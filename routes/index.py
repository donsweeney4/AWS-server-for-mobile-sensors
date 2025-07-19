import os
import io
import zipfile
import re
import datetime
import asyncio
import logging

from quart import (
    Quart, Blueprint, render_template, jsonify, request,
    Response, session, send_file, redirect, url_for
)
import boto3
from botocore.exceptions import ClientError

from database import fetch_all_rows, execute_db_update
from utils.process_routes import mainProcessData
from config import Config

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Blueprint
bp = Blueprint('index', __name__)

# S3 Configuration
S3_BUCKET = "urban-heat-island-data"
S3_REGION = "us-west-2"
s3_client = boto3.client("s3", region_name=S3_REGION)

# Quart App
app = Quart(__name__)
app.config.from_object(Config)
app.secret_key = os.environ.get('QUART_SECRET_KEY', 'a_super_secret_key_for_dev')
if app.secret_key == 'a_super_secret_key_for_dev':
    logger.warning("QUART_SECRET_KEY not set. Using default -- change in production!")


# --- S3 Helpers (synchronous boto3 offloaded to thread) ---
async def list_s3_files(bucket_name):
    if not s3_client:
        return {'error': 'S3 client not initialized.'}, 500
    if not bucket_name:
        return {'error': 'S3 bucket name not configured.'}, 500
    try:
        resp = await asyncio.to_thread(
            s3_client.list_objects_v2,
            Bucket=bucket_name
        )
        files = [obj['Key'] for obj in resp.get('Contents', [])]
        return files, 200
    except ClientError as e:
        code = e.response.get('Error', {}).get('Code')
        if code == 'NoSuchBucket':
            return {'error': f"Bucket '{bucket_name}' not found."}, 404
        if code == 'AccessDenied':
            return {'error': 'Access denied to S3 bucket.'}, 403
        return {'error': f"S3 error: {e}"}, 500
    except Exception as e:
        return {'error': f"Unexpected error listing files: {e}"}, 500


async def download_s3_file(bucket_name, key):
    if not s3_client:
        raise RuntimeError('S3 client not initialized.')
    if not bucket_name:
        raise RuntimeError('S3 bucket name not configured.')
    try:
        resp = await asyncio.to_thread(
            s3_client.get_object,
            Bucket=bucket_name,
            Key=key
        )
        body = resp['Body']
        data = await asyncio.to_thread(body.read)
        return data
    except ClientError as e:
        code = e.response.get('Error', {}).get('Code')
        if code == 'NoSuchKey':
            raise FileNotFoundError(f"Key '{key}' not found in '{bucket_name}'.")
        if code == 'AccessDenied':
            raise PermissionError(f"Access denied for '{key}'.")
        raise
    except Exception as e:
        raise RuntimeError(f"Error downloading '{key}': {e}")


# --- Routes ---

@bp.route('/health')
def health():
    return 'OK', 200


@bp.route('/')
async def index():
    logger.info('Rendering index page')
    return await render_template('index.html')


@bp.route('/uploadCSV')
async def uploadCSV():
    logger.info('Rendering uploadCSV page')
    return await render_template('uploadCSV.html')


@bp.route('/get_presigned_url', methods=['POST'])
async def get_presigned_url():
    try:
        data = await request.get_json()
        filename = data.get('filename')
        if not s3_client:
            return jsonify({'error': 'S3 client not initialized.'}), 500
        if not filename or not re.match(r'^.+_\d{3}\.csv$', filename):
            logger.warning(f"Invalid filename: {filename}")
            return jsonify({'error': 'Filename must end with _NNN.csv'}), 400
        key = filename
        presigned = await asyncio.to_thread(
            s3_client.generate_presigned_url,
            ClientMethod='put_object',
            Params={
                'Bucket': S3_BUCKET,
                'Key': key,
                'ContentType': 'text/csv'
            },
            ExpiresIn=600
        )
        public = f"https://{S3_BUCKET}.s3.{S3_REGION}.amazonaws.com/{key}"
        logger.info(f"Generated presigned URL for {key}")
        return jsonify({'uploadUrl': presigned, 'publicUrl': public})
    except Exception as e:
        logger.exception('Error generating presigned URL')
        return jsonify({'error': str(e)}), 500


@bp.route('/renderprocessdata')
async def renderprocessdata():
    session.setdefault('start_time_adjustment_minutes', 1.0)
    session.setdefault('end_time_adjustment_minutes', 1.0)
    session.setdefault('cutoff_speed_MPH', 1.0)
    session.setdefault('slope_option', 1)
    session.setdefault('temperature_drift_f', 0.0)
    session.setdefault('min_q', 3)
    session.setdefault('max_q', 97)
    session.setdefault('solid_color', False)
    logger.info('Rendering processdata')
    return await render_template('renderprocessdata.html', **session)


@bp.route('/run_processing', methods=['POST'])
async def run_processing():
    logger.info('📥 Received POST at /run_processing')
    campaign_id = session.get('campaign_id')
    if not campaign_id:
        return Response('Campaign ID is missing.', status=400)
    data = await request.get_json()
    if not data:
        return Response('No input data provided.', status=400)

    # Update session with inputs
    session['start_time_adjustment_minutes'] = data.get(
        'start_time_adjustment_minutes', session['start_time_adjustment_minutes']
    )
    session['end_time_adjustment_minutes'] = data.get(
        'end_time_adjustment_minutes', session['end_time_adjustment_minutes']
    )
    session['cutoff_speed_MPH'] = data.get(
        'cutoff_speed_MPH', session['cutoff_speed_MPH']
    )
    session['slope_option'] = data.get(
        'slope_option', session['slope_option']
    )
    session['temperature_drift_f'] = data.get(
        'temperature_drift_f', session['temperature_drift_f']
    )
    session['min_q'] = data.get(
        'color_table_min_quantile', session['min_q']
    )
    session['max_q'] = data.get(
        'color_table_max_quantile', session['max_q']
    )
    session['solid_color'] = bool(data.get(
        'solid_color', session['solid_color']
    ))

    try:
        root_name = data.get('process_id', campaign_id)
        def parse_float(f, d): return float(data.get(f, d))
        def parse_int(f, d): return int(data.get(f, d))

        # Parse all parameters
        start_adj = parse_float('start_time_adjustment_minutes', session['start_time_adjustment_minutes'])
        end_adj = parse_float('end_time_adjustment_minutes', session['end_time_adjustment_minutes'])
        cutoff = parse_float('cutoff_speed_MPH', session['cutoff_speed_MPH'])
        slope = parse_int('slope_option', session['slope_option'])
        temp_drift_in = parse_float('temperature_drift_f', session['temperature_drift_f'])
        min_q = parse_int('color_table_min_quantile', session['min_q'])
        max_q = parse_int('color_table_max_quantile', session['max_q'])
        solid = session['solid_color']

        logger.info(f"Calling mainProcessData for {root_name}")
        (temp_drift_out, duration_s,
         max_temp_corr, max_temp, min_temp) = await asyncio.to_thread(
            mainProcessData,
            root_name=root_name,
            start_time_adjustment_minutes=start_adj,
            end_time_adjustment_minutes=end_adj,
            cutoff_speed_MPH=cutoff,
            slope_option=slope,
            temperature_drift_f=temp_drift_in,
            color_table_min_quantile=min_q,
            color_table_max_quantile=max_q,
            solid_color=solid
        )

        # Store results
        session['temperature_drift_f'] = round(temp_drift_out, 6)
        session['campaign_duration_minutes'] = round(duration_s / 60, 2)
        session['maximum_temperature_correction_f'] = round(max_temp_corr, 3)
        session['max_corrected_temperature_f'] = round(max_temp, 2)
        session['min_corrected_temperature_f'] = round(min_temp, 2)

        return jsonify({
            'status': 'ok',
            'temperature_drift_f': session['temperature_drift_f'],
            'campaign_duration_minutes': session['campaign_duration_minutes'],
            'maximum_temperature_correction_f': session['maximum_temperature_correction_f'],
            'max_corrected_temperature_f': session['max_corrected_temperature_f'],
            'min_corrected_temperature_f': session['min_corrected_temperature_f']
        })
    except Exception as e:
        logger.exception('Error in /run_processing')
        return jsonify({'status': 'error', 'message': str(e)}), 500


@bp.route('/map')
async def map_view():
    campaign_id = session.get('campaign_id')
    if not campaign_id:
        return Response('Campaign ID missing.', status=400)
    key = f"{campaign_id}_color_coded_temperature_map.html"
    try:
        data = await download_s3_file(S3_BUCKET, key)
        html = data.decode('utf-8')
        return Response(html, content_type='text/html')
    except FileNotFoundError:
        return Response('Map file not found.', status=404)
    except Exception as e:
        return Response(f'Error fetching map: {e}', status=500)


@bp.route('/temperatureplot')
async def temperature_plot():
    campaign_id = session.get('campaign_id')
    if not campaign_id:
        return Response('Campaign ID missing.', status=400)
    key = f"{campaign_id}_fig1_corrected_temperature_map_time_window.html"
    try:
        data = await download_s3_file(S3_BUCKET, key)
        html = data.decode('utf-8')
        return Response(html, content_type='text/html')
    except FileNotFoundError:
        return Response('Plot file not found.', status=404)
    except Exception as e:
        return Response(f'Error fetching plot: {e}', status=500)


@bp.route('/get_campaign_locations', methods=['POST'])
async def get_campaign_locations():
    data = await request.get_json()
    show_hidden = data.get('show_hidden', False)
    sql = "SELECT campaign_id, campaign_title, owners, run_date, hidden FROM mobile_metadata"
    if not show_hidden:
        sql += " WHERE hidden = 0"
    sql += " ORDER BY run_date DESC"
    rows = await asyncio.to_thread(fetch_all_rows, sql)
    serial = []
    for r in rows:
        d = {}
        for k, v in r.items():
            if isinstance(v, (datetime.date, datetime.datetime)):
                d[k] = v.isoformat()
            elif k == 'hidden':
                d[k] = bool(v)
            else:
                d[k] = v
        serial.append(d)
    return jsonify({'campaigns': serial, 'selected': session.get('campaign_id')})


@bp.route('/update_campaign', methods=['POST'])
async def update_campaign():
    try:
        c = await request.get_json()
        cid = c.get('campaign_id')
        title = c.get('campaign_title')
        owners = c.get('owners')
        rd = c.get('run_date')
        hidden = c.get('hidden')
        if not cid:
            return jsonify({'error': 'Missing campaign_id'}), 400
        if rd:
            datetime.datetime.strptime(rd, '%Y-%m-%d')
        hid = 1 if hidden else 0
        sql = (
            "UPDATE mobile_metadata "
            "SET campaign_title=%s, owners=%s, run_date=%s, hidden=%s "
            "WHERE campaign_id=%s"
        )
        params = (title, owners, rd, hid, cid)
        ok = await asyncio.to_thread(execute_db_update, sql, params)
        if ok:
            return jsonify({'message': f'Campaign {cid} updated.'}), 200
        return jsonify({'error': 'DB update failed.'}), 500
    except Exception as e:
        logger.exception('Error in /update_campaign')
        return jsonify({'error': str(e)}), 500


@bp.route('/update_hidden', methods=['POST'])
async def update_hidden():
    try:
        c = await request.get_json()
        cid = c.get('campaign_id')
        hidden = c.get('hidden')
        if cid is None or hidden is None:
            return jsonify({'error': 'Missing data'}), 400
        if not isinstance(hidden, bool):
            return jsonify({'error': "'hidden' must be boolean"}), 400
        hid = 1 if hidden else 0
        sql = "UPDATE mobile_metadata SET hidden=%s WHERE campaign_id=%s"
        params = (hid, cid)
        ok = await asyncio.to_thread(execute_db_update, sql, params)
        if ok:
            return jsonify({'message': 'Hidden updated.'}), 200
        return jsonify({'error': 'DB update failed.'}), 500
    except Exception as e:
        logger.exception('Error in /update_hidden')
        return jsonify({'error': str(e)}), 500


@bp.route('/registration')
async def registration():
    return await render_template('newRegistration.html')

@bp.route('/arcgis')
async def arcgis():
    return await render_template('arcgis.html')

########################################################################
## Metadata routes

async def _upload_metadata_html(cid: str):
    # Helper function Fetch metadata for the given campaign ID
    # so the metadata page is saved to an S3 file

    # Re-fetch metadata for the campaign after processing
    rows = await asyncio.to_thread(
        fetch_all_rows,
        "SELECT * FROM mobile_metadata WHERE campaign_id = %s",
        (cid,)
    )
    if not rows:
        raise RuntimeError(f"No metadata for campaign {cid} to upload")
    row = rows[0]

    # normalize dates & booleans
    for k, v in list(row.items()):
        if isinstance(v, (datetime.date, datetime.datetime)):
            row[k] = v.isoformat()
        if k == "hidden":
            row[k] = bool(v)

    #  Render to a string
    html = await render_template("metadata.html", **row)

    #  Upload
    try:
        await asyncio.to_thread(
            s3_client.put_object,
            Bucket=S3_BUCKET,
            Key=f"{cid}_metadata.html",
            Body=html.encode("utf-8"),
            ContentType="text/html",
        )
    except ClientError as e:
        logger.error(f"Failed uploading metadata HTML for {cid}: {e}")
        # you can choose to swallow or re‑raise
        raise

@bp.route('/metadata')
# Render metadata page for the current campaign
# Expects session to have 'campaign_id'
# Returns rendered metadata.html template
async def metadata():
    cid = session.get('campaign_id')
    if not cid:
        return jsonify({'error': 'Missing campaign_id'}), 400

    # fetch the row
    rows = await asyncio.to_thread(
        fetch_all_rows,
        "SELECT * FROM mobile_metadata WHERE campaign_id = %s",
        (cid,)
    )
    if not rows:
        return "No metadata for this campaign", 404

    row = rows[0]
    # normalize dates & booleans
    for k, v in list(row.items()):
        if isinstance(v, (datetime.date, datetime.datetime)):
            row[k] = v.isoformat()
        if k == 'hidden':
            row[k] = bool(v)

    # now pass each field into Jinja so your form inputs can pick them up
    return await render_template('metadata.html', **row)


@bp.route('/get_metadata', methods=['POST'])
# Fetch metadata from database for the current campaign Returns JSON with metadata fields
async def get_metadata():
    cid = session.get('campaign_id')
    if not cid:
        return jsonify({'error': 'Missing campaign_id'}), 400
    sql = "SELECT * FROM mobile_metadata WHERE campaign_id=%s"
    rows = await asyncio.to_thread(fetch_all_rows, sql, (cid,))
    if not rows:
        return jsonify({'error': 'No metadata'}), 404
    r = rows[0]
    out = {}
    for k, v in r.items():
        if isinstance(v, (datetime.date, datetime.datetime)):
            out[k] = v.isoformat()
        elif k == 'hidden':
            out[k] = bool(v)
        else:
            out[k] = v
    return jsonify(out)

@bp.route('/update_metadata', methods=['POST'])
# Update metadata for the current campaign
# Expects form data with fields: campaign_title, campaign_location, short_description, owners, notes
# Returns redirect to /metadata on success, or error message
async def update_metadata():
    cid = session.get('campaign_id')
    if not cid:
        return jsonify({'error': 'Missing campaign_id'}), 400
    form = await request.form
    fields = ['campaign_title', 'campaign_location', 'short_description', 'owners', 'notes']
    updates, vals = [], []
    for f in fields:
        if f in form:
            updates.append(f"{f}=%s")
            vals.append(form.get(f))
    if not updates:
        return "No data to update.", 400
    vals.append(cid)
    sql = (
        f"UPDATE mobile_metadata SET {', '.join(updates)}, updated_at=CURRENT_TIMESTAMP "
        "WHERE campaign_id=%s"
    )
    try:
        await asyncio.to_thread(execute_db_update, sql, tuple(vals))
        await _upload_metadata_html(cid)  # Upload updated metadata HTML
        return redirect('/metadata')
    except Exception as e:
        logger.exception('Error in /update_metadata')
        return f"Error: {e}", 500

@bp.route('/submit_metadata', methods=['POST'])
# Submit metadata form data for the current campaign
# Expects form data with fields: campaign_id, campaign_title, campaign_location, short_description, owners, notes
# Returns thank you message on success
async def submit_metadata():
    form = await request.form
    cid = session.get('campaign_id')
    logger.info(f"Metadata for {cid}: {form}")
    await _upload_metadata_html(cid)
    return 'Thank you for your submission!'

#############################################################################

@bp.route('/submit_registration', methods=['POST'])
# Submit registration form data
# Expects form data with field 'campaign_id'
# Stores campaign_id in session and redirects to /registration
# Returns error if campaign_id is missing
async def submit_registration():
    form = await request.form
    cid = form.get('campaign_id')
    if cid:
        session['campaign_id'] = cid
        return redirect('/registration')
    return jsonify({'error': 'No campaign ID'}), 400


@bp.route('/download-and-zip')
# Download files from S3 that start with the campaign_id and zip and send them
async def download_and_zip_files():
    root = session.get('campaign_id')
    if not root:
        return jsonify({'error': "Missing 'campaign_id'"}), 400
    files, code = await list_s3_files(S3_BUCKET)
    if code != 200:
        return jsonify(files), code
    matches = [f for f in files if f.startswith(root)]
    if not matches:
        return jsonify({'message': f"No files starting with '{root}'"}), 404
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, 'w', zipfile.ZIP_DEFLATED) as zf:
        for key in matches:
            try:
                data = await download_s3_file(S3_BUCKET, key)
                zf.writestr(os.path.basename(key), data)
            except FileNotFoundError:
                continue
            except PermissionError as e:
                return jsonify({'error': str(e)}), 403
            except Exception as e:
                return jsonify({'error': f"Failed '{key}': {e}"}), 500
    buf.seek(0)
    filename = f"{root}_files.zip"
    return await send_file(
        buf,
        mimetype='application/zip',
        as_attachment=True,
        attachment_filename=filename
    )

# Register blueprint and run
app.register_blueprint(bp)

if __name__ == '__main__':
    if not os.environ.get('AWS_ACCESS_KEY_ID') or not os.environ.get('AWS_SECRET_ACCESS_KEY'):
        print('Warning: AWS credentials not set.')
    app.run(debug=True, port=5000)
