import datetime
from pathlib import Path

import pandas as pd
from config import _ConfBase


class ETLState:
    def __init__(self, etl_conf: _ConfBase):
        self._conf: _ConfBase = etl_conf
        self._path: Path = Path(self.conf.string_path)
        self._filename: str = self.path.name
        self._modified_time: datetime.datetime = datetime.datetime.utcfromtimestamp(
            self.path.stat().st_mtime
        )
        self._size: str = self.sizeof_fmt(self.path.stat().st_size)
        self.start_process: datetime.datetime = datetime.datetime.now()
        self.finish_process: datetime.datetime | None = None
        self._total_process: float | None = None
        self.result_df: pd.DataFrame = None

    @property
    def conf(self) -> _ConfBase:
        return self._conf

    @property
    def path(self) -> Path:
        return self._path

    @property
    def filename(self) -> str:
        return self._filename

    @property
    def latest_modified_time(self) -> datetime.datetime:
        return self._modified_time

    @property
    def size(self) -> str:
        return self._size

    @property
    def total_process(self) -> float | None:
        return (
            None
            if self.finish_process is None
            else (self.finish_process - self.start_process).total_seconds()
        )

    """
    this method only for print stdout for readability,
    on real usage should be saved as bytes
    """

    @staticmethod
    def sizeof_fmt(num: float, suffix: str = "B") -> str:
        for unit in ("", "Ki", "Mi", "Gi", "Ti", "Pi", "Ei", "Zi"):
            if abs(num) < 1024.0:
                return f"{num:3.1f}{unit}{suffix}"
            num /= 1024.0
        return f"{num:.1f}Yi{suffix}"
