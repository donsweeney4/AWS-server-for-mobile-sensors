import os

class Config:
    """
    Configuration class for the application.
    It fetches configuration settings from environment variables.  The environment variables can be set in the hypercorn service file.
    The database is the same as the one used in the stationary network server. 
    
    The table name here is campaign_locations 

                            Environment="DB_USER=uhi"
                            Environment="DB_PASSWORD=uhi"
                            Environment="DB_HOST=localhost"
                            Environment="DB_NAME=uhi"
                            Environment="DB_PORT=3306"
                            Environment="DB_AUTH_PLUGIN=mysql_native_password"
    """

    # Database configuration
    DB_CONFIG = {
        'user': os.getenv('DB_USER', 'default_user'),  # Default to 'default_user' if not set
        'password': os.getenv('DB_PASSWORD', 'default_password'),  # Default password
        'host': os.getenv('DB_HOST', 'localhost'),  # Default to 'localhost'
        'database': os.getenv('DB_NAME', 'default_db'),  # Default database name
        'port': int(os.getenv('DB_PORT', 3306)),  # Default to MySQL default port 3306
        'auth_plugin': os.getenv('DB_AUTH_PLUGIN', 'mysql_native_password')  # Default auth plugin
    }

            # API keys and secrets
            #API_KEY = os.getenv('API_KEY', 'default_api_key')  # Default API key
            #API_SECRET = os.getenv('API_SECRET', 'default_api_secret')  # Default API secret
            #STATION_ID = os.getenv('STATION_ID', 'default_station_id')  # Default station ID

    # Additional configurations
    DEBUG = os.getenv('DEBUG', 'False') == 'True'  # Convert string to boolean
    LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')  # Logging level
    ENV = os.getenv('ENV', 'production')  # Environment setting, default to 'production'

    # Example of a method to display config values (useful for debugging)
    @classmethod
    def print_config(cls):
        """
        Prints the current configuration for debugging purposes.
        """
        print("Printing Configuration...")
        print("Database Configuration:")
        for key, value in cls.DB_CONFIG.items():
            print(f"  {key}: {value}")

                #print("\nAPI Configuration:")
                #print(f"  API_KEY: {cls.API_KEY}")
                #print(f"  API_SECRET: {cls.API_SECRET}")
                #print(f"  STATION_ID: {cls.STATION_ID}")

        print("\nAdditional Configuration:")
        print(f"  DEBUG: {cls.DEBUG}")
        print(f"  LOG_LEVEL: {cls.LOG_LEVEL}")
        print(f"  ENV: {cls.ENV}")

# Uncomment this line to print configuration
if __name__ == "__main__":
    Config.print_config()

