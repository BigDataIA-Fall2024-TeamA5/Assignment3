import snowflake.connector
import os

# Load environment variables (if you have them in an .env file, or set directly in code for testing)
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER", "Aniket0610")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD", "Campnou106@0699")
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT", "VSGFWOP.ORB95421")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE", "MULTIMODEL_SYS_DATABASE")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA", "LOGIN_TABLE")

# Attempt to connect to Snowflake
try:
    conn = snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
    )
    print("Connection successful!")
    
    # Run a simple query to test permissions
    cursor = conn.cursor()
    cursor.execute("SELECT CURRENT_VERSION();")
    result = cursor.fetchone()
    print("Snowflake version:", result[0])
    
except snowflake.connector.errors.Error as e:
    print(f"Connection failed: {e}")
finally:
    # Close the connection if it was created
    if 'conn' in locals():
        conn.close()
