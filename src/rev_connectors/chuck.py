import polars as pl
import requests

from . import BaseConnector


@pl.api.register_dataframe_namespace('chuck')
class Chuck(BaseConnector):
    def __init__(self, df: pl.DataFrame) -> None:
        self._df = df

    def query(self) -> pl.DataFrame:
        random_joke = requests.get('https://api.chucknorris.io/jokes/random')
        jokes: list[dict] = [random_joke.json()]
        return pl.DataFrame(jokes)

    def create(self) -> pl.DataFrame:
        ...

    def update(self) -> pl.DataFrame:
        ...

    def upsert(self) -> pl.DataFrame:
        ...

    def delete(self) -> pl.DataFrame:
        ...
