import polars as pl
import requests

from . import BaseConnector


# Chuck Norris Joke API Connector
# This is connector is for demonstration purposes only to help guide the creation of new connectors.
@pl.api.register_dataframe_namespace('chuck')
class Chuck(BaseConnector):
    def __init__(self, df: pl.DataFrame) -> None:
        self._df = df

    def create(self) -> pl.DataFrame:
        ...

    def read(self) -> pl.DataFrame:
        random_joke = requests.get('https://api.chucknorris.io/jokes/random')
        jokes: list[dict] = [random_joke.json()]
        return pl.DataFrame(jokes)

    def update(self) -> pl.DataFrame:
        ...

    def delete(self) -> pl.DataFrame:
        ...
