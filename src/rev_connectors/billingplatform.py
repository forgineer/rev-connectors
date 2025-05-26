import polars as pl
import requests

from . import BaseConnector

@pl.api.register_dataframe_namespace('billingplatform')
class BillingPlatform(BaseConnector):
    def __init__(self, df: pl.DataFrame) -> None:
        self._df = df

    def create(self) -> pl.DataFrame:
        ...

    def read(self) -> pl.DataFrame:
        ...

    def update(self) -> pl.DataFrame:
        ...

    def delete(self) -> pl.DataFrame:
        ...
