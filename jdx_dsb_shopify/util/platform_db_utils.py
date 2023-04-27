from urllib.parse import quote

from jdx_utils.api.secrets import get_secret_from_sm


def get_platformdb_conn_str(secret_name):
    platform_db_secret = get_secret_from_sm(secret_name)
    endpoint = platform_db_secret['host']
    port = platform_db_secret['port']
    db_name = platform_db_secret['dbname']
    user = quote(platform_db_secret['username'])
    password = quote(platform_db_secret['password'])
    conn_str = f"postgresql+psycopg2://{user}:{password}@{endpoint}:{port}/{db_name}?sslmode=require"
    return conn_str
