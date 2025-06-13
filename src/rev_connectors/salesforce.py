import polars as pl
from simple_salesforce import Salesforce as SF
from typing import Protocol, Dict, Any, List
from . import BaseConnector


class SalesforceClient(Protocol):
    # Protocol defining required methods for Salesforce clients
    def query(self, soql: str, **kwargs) -> List[Dict[str, Any]]: ...


@pl.api.register_dataframe_namespace('salesforce')
class Salesforce(BaseConnector):
    def __init__(self, df: pl.DataFrame) -> None:
        self._df = df
        self._client = None

    def set_client(self, client: SalesforceClient) -> None:
        self._client = client

    def create(self) -> pl.DataFrame:
        ...

    def read(self, input: str, **kwargs) -> pl.DataFrame:
        # Execute a SOQL query using the configured client
        if not self._client:
            raise RuntimeError("No Salesforce client configured. Call set_client() first.")
        try:
            results = self._client.query(input, **kwargs)
            # Parse attributes if present
            df = pl.DataFrame(results)
            if 'attributes' in df.columns:
                df = df.with_columns([
                    pl.col('attributes').struct.field('type').alias('sf_type'),
                    pl.col('attributes').struct.field('url').alias('sf_url')
                ]).drop('attributes')

            return df
        except Exception as e:
            raise RuntimeError(f"Failed to execute Salesforce query: {str(e)}")

    def update(self) -> pl.DataFrame:
        ...

    def delete(self) -> pl.DataFrame:
        ...


class SimpleSalesforceClient:
    # Adapter for simple-salesforce
    def __init__(self, credentials: dict):
        self.sf = SF(**credentials)
    
    def _get_sobject_from_query(self, soql: str) -> str:
        # Extract sObject name from SOQL query
        import re
        match = re.search(r'FROM\s+(\w+)', soql, re.IGNORECASE)
        if not match:
            raise ValueError("Could not determine object from query")
        return match.group(1)
    
    def query(self, soql: str, method: str = 'rest', **kwargs) -> List[Dict[str, Any]]:
        # Execute a SOQL query using specified method
        try:
            if method == 'rest':
                results = self.sf.query_all(soql)['records']
            elif method == 'bulk':
                sobject = self._get_sobject_from_query(soql)
                bulk_job = getattr(self.sf.bulk, sobject).query(soql)
                if bulk_job and isinstance(bulk_job[0], list):
                    headers = bulk_job[0]
                    data = bulk_job[1:]
                    results = [
                        dict(zip(headers, row))
                        for row in data
                    ]
                    return results
                return bulk_job
            else:
                raise ValueError(f"Invalid query method: {method}. Use 'rest' or 'bulk'")
            return results
        except Exception as e:
            raise RuntimeError(f"Failed to execute {method} query: {str(e)}")



