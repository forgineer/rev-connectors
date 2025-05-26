import polars as pl

from abc import ABC, abstractmethod

def main() -> None:
    print("Hello from rev-connectors!")

class BaseConnector(ABC):
    @abstractmethod
    def __init__(self, df: pl.DataFrame) -> None:
        ...

    @abstractmethod
    def query(self) -> pl.DataFrame:
        ...

    @abstractmethod
    def create(self) -> pl.DataFrame:
        ...

    @abstractmethod
    def update(self) -> pl.DataFrame:
        ...

    @abstractmethod
    def upsert(self) -> pl.DataFrame:
        ...

    @abstractmethod
    def delete(self) -> pl.DataFrame:
        ...
