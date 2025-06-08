import polars as pl
import pandas as pd
from simple_salesforce import Salesforce as SF
from typing import Union, Protocol, Dict, Any, List
from . import BaseConnector

# Temporary sf-api-tools import workaround
# import sys
# import os
# current_dir = os.path.dirname(os.path.abspath(__file__))
# project_root = os.path.dirname(os.path.dirname(os.path.dirname(current_dir)))
# sf_api_tools_path = os.path.join(project_root, 'sf-api-tools\\src')
# sys.path.append(sf_api_tools_path)
# import query 

DataFrame = Union[pl.DataFrame, pd.DataFrame]


class SalesforceClient(Protocol):
    """Protocol defining required methods for Salesforce clients"""
    def query(self, soql: str, **kwargs) -> List[Dict[str, Any]]: ...

class SfApiToolsClient:
    """Adapter for sf-api-tools"""
    def __init__(self, session: tuple):
        self.session = session
    
    def query(self, soql: str, method: str = 'rest', **kwargs) -> List[Dict[str, Any]]:
        """Execute a SOQL query using specified method"""
        query_methods = {
            'rest': self._query_rest,
            'bulk': self._query_bulk,
            'soap': self._query_soap
        }
        
        if method not in query_methods:
            raise ValueError(f"Invalid query method: {method}. Use {', '.join(query_methods.keys())}")
        
        return query_methods[method](soql, **kwargs)
    
    def _query_rest(self, soql: str, **kwargs) -> List[Dict[str, Any]]:
        return query.query_rest(input_query=soql, session=self.session)
    
    def _query_soap(self, soql: str, **kwargs) -> List[Dict[str, Any]]:
        return query.query_soap(input_query=soql, session=self.session)
    
    def _query_bulk(self, soql: str, **kwargs) -> List[Dict[str, Any]]:
        return query.query_bulk(input_query=soql, session=self.session)
    

class SimpleSalesforceClient:
    """Adapter for simple-salesforce"""
    def __init__(self, credentials: dict):
        self.sf = SF(**credentials)
    
    def _get_sobject_from_query(self, soql: str) -> str:
        """Extract sObject name from SOQL query"""
        import re
        match = re.search(r'FROM\s+(\w+)', soql, re.IGNORECASE)
        if not match:
            raise ValueError("Could not determine object type from query")
        return match.group(1)
    
    def query(self, soql: str, method: str = 'rest', **kwargs) -> List[Dict[str, Any]]:
        """Execute a SOQL query using specified method"""
        try:
            if method == 'rest':
                results = self.sf.query_all(soql)['records']
                for record in results:
                    if isinstance(record, dict):
                        record.pop('attributes', None)
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


@pl.api.register_dataframe_namespace('salesforce')
@pd.api.extensions.register_dataframe_accessor('salesforce')
class Salesforce(BaseConnector):
    def __init__(self, df: DataFrame) -> None:
        self._df = df
        self._client = None

    def _create_df(self, data) -> DataFrame:
        if isinstance(self._df, pl.DataFrame):
            return pl.DataFrame(data)
        return pd.DataFrame(data)
    
    def set_client(self, client: SalesforceClient) -> None:
        self._client = client

    def create(self) -> DataFrame:
        ...

    def read(self, input: str, **kwargs) -> DataFrame:
        """Execute a SOQL query using the configured client"""
        if not self._client:
            raise RuntimeError("No Salesforce client configured. Call set_client() first.")
        try:
            results = self._client.query(input, **kwargs)
            return self._create_df(results)
        except Exception as e:
            raise RuntimeError(f"Failed to execute Salesforce query: {str(e)}")

    def update(self) -> DataFrame:
        ...

    def delete(self) -> DataFrame:
        ...
