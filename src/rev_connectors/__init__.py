import polars as pl

from abc import ABC, abstractmethod


class BaseConnector(ABC):
    @abstractmethod
    def __init__(self, df: pl.DataFrame) -> None:
        ...

    # Abstract methods for CRUD operations
    @abstractmethod
    def create(self) -> pl.DataFrame:
        ...

    @abstractmethod
    def read(self) -> pl.DataFrame:
        ...

    @abstractmethod
    def update(self) -> pl.DataFrame:
        ...

    @abstractmethod
    def delete(self) -> pl.DataFrame:
        ...