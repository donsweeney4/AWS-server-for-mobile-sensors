import logging
import asyncio
import datetime
from quart import Blueprint, jsonify, request, session, redirect
from database import fetch_all_rows, execute_db_update # Assuming these are properly implemented

# Define blueprint
campaigns_bp = Blueprint('campaigns', __name__)

logger = logging.getLogger(__name__)

##//#############################################################################
@campaigns_bp.route('/get_campaign_names', methods=['POST'])
async def get_campaign_names():
    """
    Fetches campaign metadata from the database.
    Filters by the location in the session (if it exists).
    Can filter campaigns based on the 'show_hidden' flag.
    """
    logger.info(f"Fetching rows from mobile_metadata...")

    data = await request.get_json()
    show_hidden_campaigns = data.get('show_hidden', False)

    
    selected_location = session.get('selected_location') 


    base_query = "SELECT campaign_id, campaign_title, owners, run_date, hidden, campaign_location FROM mobile_metadata"
    where_clauses = []  # <-- NEW
    params = []         # <-- NEW

 
    if selected_location:

        where_clauses.append("campaign_location = %s") 
        params.append(selected_location)               
    else:
        logger.warning("No location in session. Showing campaigns from all locations.") # <-- NEW

    #  Add hidden filter (now with parameters)
    if not show_hidden_campaigns:
        where_clauses.append("hidden = %s") # <-- UPDATED
        params.append(0)                    # <-- NEW
    
    #  Assemble the final query
    sql_query = base_query
    if where_clauses:
        sql_query += " WHERE " + " AND ".join(where_clauses) # <-- NEW
    
    sql_query += " ORDER BY run_date DESC"

    logger.info(f"Executing query: {sql_query} with params: {params}") # <-- UPDATED

    #  Execute the query with the new parameters
    rows = await asyncio.to_thread(fetch_all_rows, sql_query, tuple(params))
    
    # --- END NEW LOGIC ---

    logger.info(f"✅ Number of rows fetched: {len(rows)}")
    
    
    rows_serializable = []
    for row in rows:
        processed_row = {}
        for key, value in row.items():
            if isinstance(value, (datetime.date, datetime.datetime)):
                processed_row[key] = value.isoformat()
            elif key == 'hidden':
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
@campaigns_bp.route('/set_campaign_session', methods=['POST'])
async def set_campaign_session():
    """
    Receives a campaign ID from the frontend and stores it in the session.
    """
    try:
        data = await request.get_json()
        campaign_id = data.get('campaign_id')

        if not campaign_id:
            logger.warning("No campaign_id received in /set_campaign_session")
            return jsonify({"error": "No campaign_id provided"}), 400

        # Store the selected ID in the server-side session
        session['campaign_id'] = campaign_id
        logger.info(f"Set session campaign_id to: {campaign_id}")
        
        return jsonify({"status": "ok", "campaign_id": campaign_id}), 200

    except Exception as e:
        logger.exception(f"Error in /set_campaign_session: {e}")
        return jsonify({"error": str(e)}), 500




 ##//#############################################################################
@campaigns_bp.route('/update_campaign', methods=['POST'])
async def update_campaign():
    """
    Updates specific fields for a campaign in the 'mobile_metadata' table.
    """
    try:
        campaign_data = await request.get_json() 
        logger.info(f"🔵 Received single campaign update: {campaign_data}")

        campaign_id = campaign_data.get('campaign_id')
        campaign_title = campaign_data.get('campaign_title')
        owners = campaign_data.get('owners')
        run_date_str = campaign_data.get('run_date')
        hidden = campaign_data.get('hidden')

        if not campaign_id:
            return jsonify({"error": "Missing campaign_id"}), 400

        if run_date_str:
            try:
                datetime.datetime.strptime(run_date_str, '%Y-%m-%d')
            except ValueError:
                return jsonify({"error": f"Invalid run_date format: {run_date_str}. Use YYYY-MM-DD."}), 400
        else:
            run_date_str = None 

        hidden_int = 1 if hidden else 0

        sql = """
            UPDATE mobile_metadata
            SET campaign_title = %s, owners = %s, run_date = %s, hidden = %s
            WHERE campaign_id = %s
        """
        params = (campaign_title, owners, run_date_str, hidden_int, campaign_id)
        logger.info(f"🔵 Executing SQL: {sql} with params: {params}")

        success = await asyncio.to_thread(execute_db_update, sql, params)

        if success:
            logger.info(f"✅ Successfully updated campaign {campaign_id}")
            return jsonify({"message": f"Campaign {campaign_id} updated successfully."}), 200
        else:
            logger.warning(f"⚠️ Update function returned false for {campaign_id}.")
            return jsonify({"error": "Database update failed."}), 500

    except Exception as e:
        logger.exception("❌ Exception while updating campaign in /update_campaign route")
        return jsonify({"error": "An error occurred", "details": str(e)}), 500

##//#############################################################################
@campaigns_bp.route('/update_hidden', methods=['POST'])
async def update_hidden():
    """
    Updates only the 'hidden' status for a campaign in the 'mobile_metadata' table.
    """
    try:
        campaign_data = await request.get_json() 
        logger.info(f"🔵 Received hidden status update: {campaign_data}")

        campaign_id = campaign_data.get('campaign_id')
        hidden = campaign_data.get('hidden') 

        if not campaign_id:
            return jsonify({"error": "Missing campaign_id"}), 400

        if hidden is None or not isinstance(hidden, bool):
            return jsonify({"error": "Missing or invalid 'hidden' status. Must be a boolean."}), 400

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
            logger.warning(f"⚠️ Update function returned false for hidden status update of {campaign_id}.")
            return jsonify({"error": "Database update failed for hidden status."}), 500

    except Exception as e:
        logger.exception("❌ Exception while updating hidden status in /update_hidden route")
        return jsonify({"error": "An error occurred", "details": str(e)}), 500

##//#############################################################################
@campaigns_bp.route('/get_metadata', methods=['POST'])
async def get_metadata(): 
    """
    Fetches detailed metadata for the selected campaign ID from mobile_metadata.
    """
    campaign_id = session.get("campaign_id")
    if not campaign_id:
        logger.warning("No campaign_id in session for get_metadata.")
        return jsonify({"error": "Campaign ID missing from session."}), 400

    query = "SELECT * FROM mobile_metadata WHERE campaign_id = %s"
    rows = await asyncio.to_thread(fetch_all_rows, query, (campaign_id,))
    
    if not rows:
        logger.warning(f"No metadata found for campaign_id: {campaign_id}")
        return jsonify({"error": f"No metadata found for campaign ID: {campaign_id}"}), 404

    result = {}
    for key, value in rows[0].items():
        if isinstance(value, (datetime.datetime, datetime.date)):
            result[key] = value.isoformat()
        elif key == 'hidden':
             result[key] = bool(value) 
        else:
            result[key] = value

    logger.info(f"Fetched metadata for campaign_id: {campaign_id}")
    return jsonify(result)

##//#############################################################################
@campaigns_bp.route('/update_metadata', methods=['POST'])
async def update_metadata():
    """
    Updates editable metadata fields for a campaign in the 'mobile_metadata' table.
    """
    campaign_id = session.get("campaign_id")
    if not campaign_id:
        logger.warning("No campaign_id in session for update_metadata.")
        return jsonify({"error": "Campaign ID missing from session."}), 400

    form = await request.form 
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
    
    query = f"""
        UPDATE mobile_metadata
        SET {", ".join(updates)},
            updated_at = CURRENT_TIMESTAMP
        WHERE campaign_id = %s
    """

    try:
        await asyncio.to_thread(execute_db_update, query, tuple(values))
        logger.info(f"Successfully updated metadata for campaign_id: {campaign_id}")
        return redirect("/metadata") 
    except Exception as e:
        logger.exception(f"Error updating metadata for campaign_id: {campaign_id}")
        return f"Error updating metadata: {str(e)}", 500