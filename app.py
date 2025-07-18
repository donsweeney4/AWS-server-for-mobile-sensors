import logging
import os
from quart import Quart, session
from config import Config
from routes.index import bp as index_bp
import asyncio
from quart_cors import cors

 



# initialize global variables



#############################################################################

# Set up logging - no file writing in app, only console output, with color support
# view log data on terminal  with:  
#                  journalctl -u myapp-mobilesensors.service --no-pager -f


logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

console_handler = logging.StreamHandler()
console_handler.setLevel(logging.DEBUG)

try:
    from colorlog import ColoredFormatter
    color_format = '%(log_color)s%(asctime)s %(levelname)s %(name)s: %(message)s'
    color_formatter = ColoredFormatter(
        color_format,
        log_colors={
            'DEBUG': 'cyan',
            'INFO': 'green',
            'WARNING': 'yellow',
            'ERROR': 'red',
            'CRITICAL': 'bold_red',
        }
    )
    console_handler.setFormatter(color_formatter)
except ImportError:
    log_format = '%(asctime)s %(levelname)s %(name)s: %(message)s'
    console_handler.setFormatter(logging.Formatter(log_format))

logger.addHandler(console_handler)


#############################################################################

# Initialize the Quart app
app = Quart(__name__)
app = cors(app, allow_origin="*")   # Allow all origins for CORS

# Secret key for sessions (for development purposes only)
app.secret_key = 'dons_secret-key'  # Set the secret key here

# Load configuration from the Config class
app.config.from_object(Config)

# Register blueprints
app.register_blueprint(index_bp)




# Run the application (for development/testing only)

# The following lines are not required if run with hypercorn but useful for running locally
# This will bypass hypercorn and nginx

#if __name__ == '__main__':
#    asyncio.run(app.run_task(debug=True, host='0.0.0.0', port=8000))
