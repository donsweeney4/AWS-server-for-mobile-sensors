import logging
from quart import Blueprint, render_template, session, redirect, request, jsonify

# Define blueprint
main_bp = Blueprint('main', __name__)

logger = logging.getLogger(__name__)

html_template = "index.html" # Default HTML template for the root route

##//####### Health check ######################################################
@main_bp.route('/health')
def health():
    """Simple health check endpoint."""
    return "OK", 200

##//#####  RENDERING ROUTES ###############################################

@main_bp.route('/')
async def index():
    """Renders the main index page."""
    logger.info("Rendering index page")
    return await render_template(html_template)

##//#############################################################################
@main_bp.route('/uploadCSV')
async def uploadCSV():
    """Renders the CSV upload page."""
    logger.info("Rendering uploadCSV page")
    return await render_template("uploadCSV.html")

##//#############################################################################
@main_bp.route('/renderprocessdata')
async def renderprocessdata():
    """
    Renders the data processing configuration page.
    Sets default session values if not already present.
    """
    session.setdefault('start_time_adjustment_minutes', 1.0)
    session.setdefault('end_time_adjustment_minutes', 1.0)
    session.setdefault('cutoff_speed_MPH', 1.0)
    session.setdefault('slope_option', 1)
    session.setdefault('temperature_drift_f', 0.0)
    session.setdefault('min_q', 3)
    session.setdefault('max_q', 97)
    session.setdefault('solid_color', False)

    logger.info("Rendering processdata page")
    
    # Pass session values as keyword arguments to the template
    return await render_template("renderprocessdata.html", **session)  

##//#############################################################################
@main_bp.route('/location')
async def location():
    """Renders the registration page."""
    logger.info("Location route hit")
    return await render_template("selectLocation.html")  

#//#############################################################################
@main_bp.route('/campaign')
async def campaign():
    logger.info("Campaign route hit")
    return await render_template("campaign.html")
 
##//#############################################################################
@main_bp.route('/metadata')
async def metadata():
    """Renders the metadata page."""
    logger.info("Metadata route hit")
    return await render_template("metadata.html")

##//#############################################################################
@main_bp.route('/arcgis')
async def arcgis():
    """Renders the ArcGIS integration page."""
    logger.info("arcgis route hit")
    return await render_template("arcgis.html")

##//#############################################################################
@main_bp.route('/store_selected_location', methods=['POST'])
async def store_selected_location():
    """
    Handles the submission of a selected location from the form in location.html.
    Stores the selected location in the session and redirects to the campaign page.
    """
    logger.info("📥 /store_selected_location route accessed")
    form_data = await request.form 
    logger.info(f"Form data received: {form_data}")
    selected_location = form_data.get('selected_location')

    if selected_location:
        session['selected_location'] = selected_location
        logger.info(f"Stored selected_location in session: {selected_location}")
        return redirect('/campaign')
    else:
        logger.warning("No location provided in form data.")
        return jsonify({"error": "No location provided for location."}), 400

##//#############################################################################
@main_bp.route('/submit_metadata', methods=['POST'])
async def submit_metadata():
    """
    Handles the submission of metadata. This route currently only logs and returns a message.
    """
    logger.info("📥 /submit_metadata route accessed")
    form_data = await request.form 
    campaign_id = session.get('campaign_id')
    logger.info(f"Received metadata for campaign {campaign_id}: {form_data}")
    return "Thank you for entering metadata!"

##//#############################################################################
@main_bp.route('/log_activity', methods=['POST'])
async def log_activity():
    """
    Logs a message from the frontend.
    """
    try:
        # Get the JSON data sent from the browser
        data = await request.get_json()

        # Log it to your server's log file (journalctl)
        if data and data.get('message'):
            logger.info(f"[FRONTEND LOG]: {data.get('message')}")
        else:
            logger.warning("[FRONTEND LOG]: Received log call with no message.")

        return jsonify({"status": "logged"}), 200

    except Exception as e:
        logger.error(f"Error in /log_activity route: {e}")
        return jsonify({"error": "Failed to log activity"}), 500