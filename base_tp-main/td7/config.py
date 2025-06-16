from dotenv import dotenv_values

config = dotenv_values(".env")

POSTGRES_USER = config["POSTGRES_USER"]
POSTGRES_PASSWORD = config["POSTGRES_PASSWORD"]
POSTGRES_DB = config["POSTGRES_DB_NAME"]

POSTGRES_CONN_STRING = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@postgres/{POSTGRES_DB}"
