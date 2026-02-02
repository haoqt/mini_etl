import psycopg2
from psycopg2.extras import execute_batch


def get_connection(dsn: str):
    return psycopg2.connect(dsn)
