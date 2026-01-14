from pathlib import Path

# Project Root Directory
# Assuming this file is at src/utils/config.py, so root is two levels up
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent

# Database Config
DB_DIR = PROJECT_ROOT / "db"
DB_NAME = "subway.db"
DB_PATH = DB_DIR / DB_NAME
DB_URL = f"sqlite:///{DB_PATH}"

# Weather Database Config
WEATHER_DB_NAME = "weather.db"
WEATHER_DB_PATH = DB_DIR / WEATHER_DB_NAME
WEATHER_DB_URL = f"sqlite:///{WEATHER_DB_PATH}"

# Data Config
DATA_DIR = PROJECT_ROOT / "data"

# Output Config
OUTPUT_DIR = PROJECT_ROOT / "output"
PLOTS_DIR = OUTPUT_DIR / "plots"

# Ensure directories exist
DB_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
PLOTS_DIR.mkdir(parents=True, exist_ok=True)
DATA_DIR.mkdir(parents=True, exist_ok=True)

# Logging Config (Basic)
LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
LOG_LEVEL = "INFO"
