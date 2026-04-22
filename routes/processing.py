import logging
import asyncio
import glob
import os
import subprocess
from quart import Blueprint, jsonify, request, Response, session
from utils.process_routes import mainProcessData # Assuming this is properly implemented

# Define blueprint
processing_bp = Blueprint('processing', __name__)

logger = logging.getLogger(__name__)

##//#############################################################################
## Route that will call python code to process the temperature data  ####################################

@processing_bp.route('/run_processing', methods=['POST'])
async def run_processing():
    """
    Triggers the data processing for a selected campaign based on provided parameters.
    Updates session with processed results.
    """
    logger.info("📥 Received POST at /run_processing")

    # Get campaign_id from session
    campaign_id = session.get('campaign_id')
    if not campaign_id:
        logger.error("❌ Campaign ID missing from session for processing.")
        return Response("Campaign ID is missing from session. Please select a campaign first.", status=400, content_type='text/plain')

    # --- NEW ---
    # Get the location (which is the bucket name) from the session
    bucket_name = session.get('selected_location')
    if not bucket_name:
        logger.error("❌ Location (bucket name) missing from session for processing.")
        return Response("Location is missing from session. Please select a location first.", status=400, content_type='text/plain')
    # --- END NEW ---

    data = await request.get_json()
    if not data:
        logger.error("❌ No JSON body in request for /run_processing.")
        return Response("No input data provided for processing.", status=400, content_type='text/plain')

    logger.info(f"🧾 Payload received for processing: {data}")

    # Update session with user-provided values
    session['start_time_adjustment_minutes'] = data.get('start_time_adjustment_minutes', session.get('start_time_adjustment_minutes'))
    session['end_time_adjustment_minutes'] = data.get('end_time_adjustment_minutes', session.get('end_time_adjustment_minutes'))
    session['cutoff_speed_MPH'] = data.get('cutoff_speed_MPH', session.get('cutoff_speed_MPH'))
    session['slope_option'] = data.get('slope_option', session.get('slope_option'))
    session['temperature_drift_f'] = data.get('temperature_drift_f', session.get('temperature_drift_f'))
    session['min_q'] = data.get('color_table_min_quantile', session.get('min_q'))
    session['max_q'] = data.get('color_table_max_quantile', session.get('max_q'))
    session['solid_color'] = data.get('solid_color', session.get('solid_color'))

    try:
        root_name = data.get('process_id', campaign_id) 

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
        cutoff_speed_MPH = parse_float('cutoff_speed_MPH', session['cutoff_speed_MPH'])
        slope_option = parse_int('slope_option', session['slope_option'])
        temperature_drift_f_input = parse_float('temperature_drift_f', session['temperature_drift_f']) / 3600  # Convert from deg F/hour to deg F/sec 
        
        # (I also fixed a typo here, removing a stray 'A' from your original file)
        color_table_min_quantile = parse_int('color_table_min_quantile', session['min_q'])
        color_table_max_quantile = parse_int('color_table_max_quantile', session['max_q'])
        solid_color = bool(data.get('solid_color', session['solid_color']))

        # --- UPDATED LOG MESSAGE ---
        logger.info(f"⚙️ Calling mainProcessData for campaign {root_name} in bucket {bucket_name} with parameters: "
                    f"start_time_adj={start_time_adjustment_minutes}, end_time_adj={end_time_adjustment_minutes}, "
                    f"cutoff_speed={cutoff_speed_MPH}, slope_option={slope_option}, "
                    f"temp_drift_input={temperature_drift_f_input}, min_q={color_table_min_quantile}, "
                    f"max_q={color_table_max_quantile}, solid_color={solid_color}")

        # Use asyncio.to_thread for the blocking mainProcessData function
        temperature_drift_f_output, campaign_duration_seconds, maximum_temperature_correction_f,max_corrected_temperature_f,min_corrected_temperature_f = \
            await asyncio.to_thread(mainProcessData,
                                    root_name=root_name,
                                    bucket_name=bucket_name, # <-- NOW CORRECTLY PASSED
                                    start_time_adjustment_minutes=start_time_adjustment_minutes,
                                    end_time_adjustment_minutes=end_time_adjustment_minutes,
                                    cutoff_speed_MPH=cutoff_speed_MPH,
                                    slope_option=slope_option,
                                    temperature_drift_f=temperature_drift_f_input,
                                    color_table_min_quantile=color_table_min_quantile,
                                    color_table_max_quantile=color_table_max_quantile,
                                    solid_color=solid_color)

        # Update session with results
        session['temperature_drift_f'] = round(temperature_drift_f_output, 6)
        session['campaign_duration_minutes'] = round(campaign_duration_seconds / 60, 2)
        session['maximum_temperature_correction_f'] = round(maximum_temperature_correction_f, 3)
        session['max_corrected_temperature_f'] = round(max_corrected_temperature_f,2)
        session['min_corrected_temperature_f'] = round(min_corrected_temperature_f,2)

        logger.info("✅ mainProcessData completed successfully")
        return jsonify({
            "status": "ok",
            "message": "Processing completed successfully",
            "temperature_drift_f": round(session.get("temperature_drift_f") * 3600, 3),  # Convert from deg F/sec to deg F/hour with 3 decimal places
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
## Route that runs the recursive subsampling script  ###########################

@processing_bp.route('/run_subsampling', methods=['POST'])
async def run_subsampling():
    """
    Finds the single CSV file in ./temporary_data/ and runs
    Recursive_Sub_Sample_Script_V9.py with it as the --csv argument.
    """
    logger.info("📥 Received POST at /run_subsampling")

    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    tmp_dir = os.path.join(base_dir, 'temporary_data')
    csv_files = glob.glob(os.path.join(tmp_dir, '*.csv'))

    if not csv_files:
        return jsonify({"status": "error", "message": "No CSV file found in temporary_data/"}), 400
    if len(csv_files) > 1:
        return jsonify({"status": "error", "message": f"Multiple CSV files found in temporary_data/: {[os.path.basename(f) for f in csv_files]}"}), 400

    csv_path = csv_files[0]
    script_path = os.path.join(base_dir, 'utils', 'Recursive_Sub_Sample_Script_V9.py')

    # Read optional parameters from request body
    params = {}
    try:
        body = await request.get_json(silent=True)
        if body:
            params = body
    except Exception:
        pass

    cmd = ['python3', script_path, '--csv', csv_path]

    if 'min_samples' in params:
        cmd += ['--min-samples', str(int(params['min_samples']))]
    if 'min_cell_samples' in params:
        cmd += ['--min-cell-samples', str(int(params['min_cell_samples']))]
    if 'predicate' in params and params['predicate'] in ('intersects', 'within'):
        cmd += ['--predicate', params['predicate']]
    if 'color_table' in params:
        cmd += ['--color-table', str(int(params['color_table']))]
    if params.get('show_borders'):
        cmd += ['--show-borders']
    if params.get('no_static_plots'):
        cmd += ['--no-static-plots']

    logger.info(f"Running subsampling script: {' '.join(cmd)}")

    def run_script():
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            cwd=base_dir
        )
        return result

    try:
        result = await asyncio.to_thread(run_script)
        if result.returncode == 0:
            logger.info("Subsampling script completed successfully")
            return jsonify({"status": "ok", "message": "Subsampling completed successfully", "output": result.stdout})
        else:
            logger.error(f"Subsampling script failed: {result.stderr}")
            return jsonify({"status": "error", "message": "Subsampling script failed", "details": result.stderr}), 500
    except Exception as e:
        logger.exception("❌ Error running subsampling script")
        return jsonify({"status": "error", "message": "An unexpected error occurred", "details": str(e)}), 500

##//#############################################################################
## Route that runs the City Traverse temperature plot script  ##################

@processing_bp.route('/run_traverse', methods=['POST'])
async def run_traverse():
    """
    Finds the single CSV file in ./temporary_data/ and runs
    city_traverse.py with it, saving outputs to traverse_output/.
    """
    logger.info("📥 Received POST at /run_traverse")

    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    tmp_dir = os.path.join(base_dir, 'temporary_data')
    csv_files = glob.glob(os.path.join(tmp_dir, '*.csv'))

    if not csv_files:
        return jsonify({"status": "error", "message": "No CSV file found in temporary_data/"}), 400
    if len(csv_files) > 1:
        return jsonify({"status": "error", "message": f"Multiple CSV files found in temporary_data/: {[os.path.basename(f) for f in csv_files]}"}), 400

    csv_path = csv_files[0]
    script_path = os.path.join(base_dir, 'utils', 'city_traverse.py')

    output_dir = os.path.join(base_dir, 'traverse_output')
    os.makedirs(output_dir, exist_ok=True)

    params = {}
    try:
        body = await request.get_json(silent=True)
        if body:
            params = body
    except Exception:
        pass

    distanceflag = int(params.get('distanceflag', 1))
    halfgraph    = params.get('halfgraph', True)

    # Pull processing parameters saved to session by renderprocessdata.html
    root_name   = session.get('campaign_id', '')
    bucket_name = session.get('selected_location', '')
    start_adj   = float(session.get('start_time_adjustment_minutes', 1.0))
    end_adj     = float(session.get('end_time_adjustment_minutes', 1.0))
    cutoff_spd  = float(session.get('cutoff_speed_MPH', 0.0))
    slope_opt   = int(session.get('slope_option', 1))
    temp_drift  = float(session.get('temperature_drift_f', 0.0))
    min_q       = int(session.get('min_q', 5))
    max_q       = int(session.get('max_q', 95))
    solid_color = bool(session.get('solid_color', False))

    cmd = [
        'python3', script_path,
        '--csv', csv_path,
        '--distanceflag', str(distanceflag),
        '--halfgraph', 'true' if halfgraph else 'false',
        '--root_name',                     root_name,
        '--bucket_name',                   bucket_name,
        '--start_time_adjustment_minutes', str(start_adj),
        '--end_time_adjustment_minutes',   str(end_adj),
        '--cutoff_speed_MPH',              str(cutoff_spd),
        '--slope_option',                  str(slope_opt),
        '--temperature_drift_f',           str(temp_drift),
        '--color_table_min_quantile',      str(min_q),
        '--color_table_max_quantile',      str(max_q),
        '--solid_color',                   'true' if solid_color else 'false',
    ]

    # Patch output paths by setting env var so script saves into traverse_output/
    env = os.environ.copy()

    logger.info(f"Running traverse script: {' '.join(cmd)}")

    def run_script():
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            cwd=output_dir,
        )
        return result

    try:
        result = await asyncio.to_thread(run_script)
        if result.returncode == 0:
            logger.info("Traverse script completed successfully")
            return jsonify({"status": "ok", "message": "Analysis completed successfully", "output": result.stdout})
        else:
            logger.error(f"Traverse script failed: {result.stderr}")
            return jsonify({"status": "error", "message": "Traverse script failed", "details": result.stderr}), 500
    except Exception as e:
        logger.exception("❌ Error running traverse script")
        return jsonify({"status": "error", "message": "An unexpected error occurred", "details": str(e)}), 500