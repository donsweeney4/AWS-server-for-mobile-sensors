
import os
import logging
import boto3
from quart import Quart
from botocore.exceptions import ClientError

# 1. Import all your new blueprints
from routes.main import main_bp
from routes.campaigns import campaigns_bp
from routes.processing import processing_bp
from routes.files import files_bp

# 2. Configure logging ONCE for the whole app
#logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)

def create_app():
    """
    Application factory to create and configure the Quart app.
    """
    app = Quart(__name__)

    # 3. Set up configuration
    app.secret_key = os.environ.get('QUART_SECRET_KEY')
    
    # Add a check for the secret key
    if not app.secret_key:
        logger.critical("QUART_SECRET_KEY is not set! The app will not run.")
        exit(1) # Exit if the key is missing

    #  Load ALL the correct S3 variables from your .service file
    app.config['S3_REGION'] = "us-west-2"
    app.config['S3_BUCKET_UPLOADS'] = os.environ.get("S3_BUCKET_UPLOADS")
    app.config['S3_BUCKET_RESULTS'] = os.environ.get("S3_BUCKET_RESULTS")
    app.config['S3_BUCKET_LOCATIONS'] = os.environ.get("S3_BUCKET_LOCATIONS")
    app.config['S3_USER_BUCKET_PREFIX'] = os.environ.get("S3_USER_BUCKET_PREFIX")
    app.config['S3_USER_BUCKET_SUFFIX'] = os.environ.get("S3_USER_BUCKET_SUFFIX")

    # Check that the critical bucket names were loaded
    if not app.config['S3_BUCKET_RESULTS'] or not app.config['S3_USER_BUCKET_PREFIX']:
        logger.error("One or more S3 bucket environment variables are not set. File operations will fail.")
        # You might want to exit(1) here

    # 4. Initialize clients and attach them to the app context
    try:
        # FIX 3: Remove key parameters. boto3 will find the IAM role automatically.
        app.s3_client = boto3.client(
            's3',
            region_name=app.config['S3_REGION']
        )
        logger.info(f"S3 client initialized for region: {app.config['S3_REGION']}")
    except Exception as e:
        logger.error(f"Error initializing S3 client: {e}")
        app.s3_client = None

    # 5. Register all blueprints
    app.register_blueprint(main_bp)
    app.register_blueprint(campaigns_bp)
    app.register_blueprint(processing_bp)
    app.register_blueprint(files_bp)
    
    logger.info("All blueprints registered.")
    return app

# Create the app instance
app = create_app()

if __name__ == '__main__':
    # 6. Run the application
    # Check for critical configs before starting
    if not app.s3_client:
        logger.critical("S3 client failed to initialize. Exiting.")
        exit(1)
        
    # FIX 3 (continued): Remove the AWS key check. It's not needed for an IAM role.
    # if not os.environ.get('AWS_ACCESS_KEY_ID') ... (REMOVED)

    app.run(debug=True, port=5000)