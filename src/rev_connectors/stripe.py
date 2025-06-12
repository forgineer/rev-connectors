# TBD
import polars as pl
import requests
from pprint import pprint
from stripe import StripeClient

from . import BaseConnector

@pl.api.register_dataframe_namespace('stripe')
class Stripe(BaseConnector):
    def __init__(self, df: pl.DataFrame) -> None:
        self._df = df

    def create(self, client: StripeClient) -> pl.DataFrame:
        ...

    def read(self, client: StripeClient, entity: str) -> pl.DataFrame:
        # Dynamically get the entity attribute from the client
        # Convert entity to lowercase to make it case-insensitive
        entity_lower = entity.lower()
        
        # Check if the entity exists in the client
        if hasattr(client, entity_lower):
            entity_resource = getattr(client, entity_lower)
            
            # List the entities
            entities = entity_resource.list()
            
            # TODO: Convert to polars DataFrame and return
            return pl.json_normalize(entities.data, max_level=3)
            # Return empty DataFrame for now
            # return pl.DataFrame()
        else:
            raise ValueError(f"Entity '{entity}' is not supported by the Stripe client")

    def update(self, client: StripeClient) -> pl.DataFrame:
        ...

    def delete(self, client: StripeClient) -> pl.DataFrame:
        ...

        
if __name__ == "__main__":
    def login(secret_key: str) -> None:
        """
        Initialize the Stripe client with the provided secret key.
        """r
        client = StripeClient(secret_key)

        return client
    
    client = login(secret_key="replace with yours")
    
    df = pl.DataFrame()
    products = df.stripe.read(client, 'products')

    pprint(products['marketing_features'])
    products.write_excel('./products.xlsx')