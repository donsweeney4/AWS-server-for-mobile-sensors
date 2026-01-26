# CLAUDE.md

## Project Overview

AWS-based Urban Heat Island (UHI) Data Server for processing and visualizing mobile sensor temperature readings. The application manages field campaign data, generates heat maps, and provides a web interface for data upload, processing, and visualization.

## Tech Stack

- **Backend**: Quart (async Python web framework)
- **Database**: MySQL (mysql-connector-python)
- **Cloud**: AWS S3 (file storage), AWS SES (email notifications), IAM roles for auth
- **Data Processing**: Pandas, NumPy, Scikit-learn, SciPy
- **Visualization**: Plotly (charts), Folium (maps), Branca (color mapping)
- **GIS**: GeoPandas, Shapely, PyKrige (kriging interpolation)
- **Server**: Hypercorn (ASGI server on port 5002)
- **Frontend**: HTML5, JavaScript, Tailwind CSS

## Project Structure

```
├── run.py                  # App factory, initializes Quart app and registers blueprints
├── config.py               # Environment-based configuration
├── database.py             # MySQL connection and query functions
├── routes/
│   ├── main.py             # Navigation routes (index, location, campaign pages)
│   ├── campaigns.py        # Campaign CRUD operations
│   ├── files.py            # S3 file operations (upload, download, zipping)
│   └── processing.py       # Data processing pipeline trigger
├── utils/
│   ├── process_routes.py   # Core processing engine (temperature corrections, maps)
│   ├── weather_data.py     # Weather data utilities
│   └── process_routes_multiple_campaigns.py  # Batch processing
├── templates/              # HTML pages (Tailwind-styled)
├── static/                 # CSS, JS, images
├── hypercorn.toml          # Web server configuration
└── createtable.sql         # Database schema
```

## Running the Application

```bash
# Development (with auto-reload)
hypercorn run:app --config hypercorn.toml --reload

# Production
hypercorn run:app --config hypercorn.toml

# Local testing
python run.py  # Runs on localhost:5000
```

## Environment Variables

Required environment variables:
- `DB_USER`, `DB_PASSWORD`, `DB_HOST`, `DB_NAME`, `DB_PORT`, `DB_AUTH_PLUGIN` - MySQL connection
- `S3_REGION`, `S3_BUCKET_UPLOADS`, `S3_BUCKET_RESULTS`, `S3_BUCKET_LOCATIONS` - AWS S3
- `S3_USER_BUCKET_PREFIX`, `S3_USER_BUCKET_SUFFIX` - S3 bucket naming
- `QUART_SECRET_KEY` - Session encryption
- `DEBUG`, `LOG_LEVEL`, `ENV` - Application settings

## Key Workflows

1. **Campaign Processing Flow**: Location selection → Campaign selection → CSV upload → Configure parameters → Process data → View/download results
2. **Data Processing**: Loads CSVs from S3 → Applies temperature corrections → Calculates temperature drift via linear regression → Generates Folium maps and Plotly charts → Stores results in S3

## Database

Single table `mobile_metadata` tracks campaign metadata. Use parameterized queries for all database operations (see `database.py`).

```bash
# Initialize database
mysql -u uhi -p uhi < createtable.sql
```

## Git Workflow

**Important**: Always follow this branching workflow when making changes:

1. **Never commit directly to `main`** - Always create a feature branch first
2. **Create a branch before making changes**:
   ```bash
   git checkout -b <type>/<short-description>
   ```
3. **Branch naming conventions**:
   - `feature/` - New features (e.g., `feature/add-export-csv`)
   - `fix/` - Bug fixes (e.g., `fix/temperature-calculation`)
   - `chore/` - Maintenance tasks (e.g., `chore/update-dependencies`)
   - `refactor/` - Code refactoring (e.g., `refactor/simplify-routes`)
   - `docs/` - Documentation updates (e.g., `docs/update-readme`)
4. **Commit changes to the feature branch**, then create a PR to merge into `main`

## Code Conventions

- Use async/await for route handlers (Quart)
- Use `asyncio.to_thread()` for blocking operations
- Use parameterized SQL queries to prevent injection
- IAM role-based authentication for AWS (no hardcoded credentials)
- Session-based state management for user workflow
- Colored console logging via colorlog

## Utility Scripts

- `find_new_campaign_ids.py` - Discover new campaigns from S3
- `ManageLogFileLength.py` - Trim log files to 5000 lines
- `s3_test_file_cleaner.py` - Clean up old S3 test files
