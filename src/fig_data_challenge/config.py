from abc import abstractmethod


class _ConfBase:
    @property
    @abstractmethod
    def clickhouse_table(self) -> str:
        raise AttributeError

    @property
    @abstractmethod
    def string_path(self) -> str:
        raise AttributeError

    @property
    @abstractmethod
    def header_list(self) -> list[str]:
        raise AttributeError


class ETLType:
    class configs:
        class RESTAURANT_DATA(_ConfBase):
            clickhouse_table = "restaurant_menu_items"
            string_path = "data/restaurant_data.xlsx"
            header_list = [
                "store",
                "product_name",
                "ingredients_on_product_page",
                "allergens_and_warnings",
                "url_of_primary_product_picture",
                "product_category",
            ]

    def __init__(self, etl_type: str):
        self.type_str: str = etl_type
        supported: bool = False
        for name, cls in vars(ETLType.configs).items():
            if self.type_str == name and isinstance(cls, type):
                self.conf = cls()
                supported = True
                break

        if not supported:
            raise ValueError("ETL Type '%s' isn't supported" % (self.type_str,))
