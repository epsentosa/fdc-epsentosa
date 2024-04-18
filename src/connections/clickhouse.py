from clickhouse_connect import get_client

client = get_client(
    host="localhost", username="default", password="", database="default", port=8123
)
