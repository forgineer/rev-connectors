import polars as pl
import pandas as pd
from typing import Union
from abc import ABC, abstractmethod

DataFrame = Union[pl.DataFrame, pd.DataFrame]


class BaseConnector(ABC):
    @abstractmethod
    def __init__(self, df: DataFrame) -> None:
        ...

    # Abstract methods for CRUD operations
    @abstractmethod
    def create(self) -> DataFrame:
        ...

    @abstractmethod
    def read(self) -> DataFrame:
        ...

    @abstractmethod
    def update(self) -> DataFrame:
        ...

    @abstractmethod
    def delete(self) -> DataFrame:
        ...
