# from fig_data_challenge.main import return_42
from connections.clickhouse import client

# def test_main():
#     value = return_42()
#     assert value == 42


def test_clickhouse_connection():
    value = client.command("SELECT 1")
    assert value == 1
