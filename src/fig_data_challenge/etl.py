import datetime
import warnings
from time import time

import pandas as pd
from config import ETLType
from state import ETLState

from connections.clickhouse import client

# hide openpyxl module warning compatibility
warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")


# normalize column header name for consistency
def neutralize_header(header: str) -> str:
    return (
        header.replace(" ", "_")
        .replace(".", "_")
        .replace(",", "_")
        .replace("-", "_")
        .replace("~", "_")
        .replace("(", "_")
        .replace(")", "_")
        .replace("%", "pct")
        .lower()
    )


def extract(etl_type: ETLType) -> ETLState:
    start_extract: float = time()
    print("starting extract")

    etl_state = ETLState(etl_type.conf)
    print("instantiate etl_state, description state:")
    print("filename             ->", etl_state.filename)
    print("size                 ->", etl_state.size)
    print("latest_modified_time ->", etl_state.latest_modified_time)

    print(f"extract finished, duration ({time() - start_extract:2.4f}) \n")
    return etl_state


def transform(etl_state: ETLState) -> None:
    start_transform: float = time()
    print("starting transform")

    column_not_null: list[str] = ["product_name", "ingredients_on_product_page"]

    df_file = pd.ExcelFile(etl_state.path)
    df_reference = df_file.parse("Reference categories")
    df_items = df_file.parse("Restaurant Menu Items")

    df_reference.columns = [
        neutralize_header(header) for header in df_reference.columns.tolist()
    ]
    """
    join string concat on category
    due to reference table have multiple key on column restaurant_name
    """
    df_reference = df_reference.groupby("restaurant_name")["fig_category_1"].apply(
        ", ".join
    )
    df_reference = df_reference.to_frame().reset_index()

    df_items.columns = [neutralize_header(header) for header in df_items.columns.tolist()]
    condition = df_items[column_not_null].notna().all(axis=1)
    df_items_filtered = df_items[condition][etl_state.conf.header_list]

    """
    fill empty value for category from reference table
    """
    df_items_filtered["product_category"] = df_items_filtered["product_category"].fillna(
        df_items_filtered["store"].map(
            df_reference.set_index("restaurant_name")["fig_category_1"]
        )
    )

    etl_state.result_df = df_items_filtered
    print(f"transform finished, duration ({time() - start_transform:2.4f}) \n")


def load(etl_state: ETLState) -> None:
    start_load: float = time()
    print("starting load")

    client.insert_df(
        table=etl_state.conf.clickhouse_table,
        df=etl_state.result_df,
    )
    print(f"load finished, duration ({time() - start_load:2.4f}) \n")


def master_etl() -> None:
    etl_type: ETLType = ETLType("RESTAURANT_DATA")
    etl_state: ETLState = extract(etl_type)

    transform(etl_state)

    load(etl_state)

    etl_state.finish_process = datetime.datetime.now()

    print("total process ->", etl_state.total_process)
