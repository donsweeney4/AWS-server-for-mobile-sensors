import logging
import aiohttp

logger = logging.getLogger(__name__)

async def fetch_QuestWeatherStation_data(session, start_timestamp, end_timestamp):
    from config import Config  # Import within function to avoid circular imports
    URL = f"https://api.weatherlink.com/v2/historic/{Config.STATION_ID}?api-key={Config.API_KEY}&start-timestamp={start_timestamp}&end-timestamp={end_timestamp}"
    logger.debug(f"Fetching data with API_KEY {Config.API_KEY} and secret {Config.API_SECRET}") 
    headers = {'X-Api-Secret': Config.API_SECRET}
    try:
        async with session.get(URL, headers=headers) as response:
            if response.status == 200:
                logger.debug(f"Successfully fetched data for timestamps {start_timestamp} to {end_timestamp}")
                return await response.json()
            else:
                logger.error(f"Error fetching data for timestamps {start_timestamp} to {end_timestamp}, status code: {response.status}")
                return None
    except Exception as e:
        logger.exception(f"Exception occurred while fetching data: {e}")
        return None

def generate_timestamps(start_date, end_date):
    from datetime import timedelta
    current_date = start_date
    while current_date < end_date:
        yield int(current_date.timestamp()), int((current_date + timedelta(days=1)).timestamp())
        current_date += timedelta(days=1)















