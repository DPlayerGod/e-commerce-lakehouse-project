import os

# Superset secret key
SECRET_KEY = os.getenv("SUPERSET_SECRET_KEY", "ecommerce-secret-dev-2026")

# Database connection
SQLALCHEMY_DATABASE_URI = os.getenv("SQLALCHEMY_DATABASE_URI")

# Flask-WTF flag for CSRF
WTF_CSRF_ENABLED = True

# Set this to True to allow users to connect to local databases
ENABLE_PROXY_FIX = True

# ClickHouse Driver is needed for connection
ADDITIONAL_MODULE_DSN_MAPPING = {
    "clickhouse": "clickhouse_connect.driver.http.ClickHouseHTTPDialect"
}
