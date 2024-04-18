from typing import Dict, List

from connections.clickhouse import client

migration_list: List[Dict[str, str]] = [
    {
        "create_table_restaurant_menu_items": """
        CREATE TABLE IF NOT EXISTS default.restaurant_menu_items
        (
           store Nullable(String),
           product_name String,
           ingredients_on_product_page String,
           allergens_and_warnings Nullable(String),
           url_of_primary_product_picture Nullable(String),
           product_category Nullable(String)
        ) ENGINE = MergeTree
              ORDER BY (product_name)
    """
    }
]


def start_db_migration() -> None:
    for migration_dict in migration_list:
        print("starting migration...")
        for migration_name, migration_command in migration_dict.items():
            print("run job:", migration_name)
            client.command(migration_command)

        print("migration finished")
