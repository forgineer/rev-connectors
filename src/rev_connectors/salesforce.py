import polars as pl
import re
import logging
from simple_salesforce import Salesforce as SF
from . import BaseConnector

logger = logging.getLogger(__name__)


@pl.api.register_dataframe_namespace('salesforce')
class Salesforce(BaseConnector):
    """Connector for Salesforce using simple_salesforce and Polars."""
    def __init__(self, credentials: dict, df: pl.DataFrame = None) -> None:
        self._df = df
        self.sf = SF(**credentials)

    def create(self) -> pl.DataFrame:
        """Not implemented."""
        ...

    def read(self, soql: str, method: str = 'rest') -> pl.DataFrame:
        """Execute a SOQL query using REST or Bulk API and return a Polars DataFrame."""
        try:
            # Query REST
            results = []
            if method == 'rest':
                results = self.sf.query_all(soql)['records']
            # Query Bulk 
            elif method == 'bulk':
                sobject = self._get_sobject_from_query(soql)
                bulk_job = getattr(self.sf.bulk, sobject).query(soql)
                # Check if the bulk job returned results
                ## Case 1: bulk_job is a list of lists (headers + rows)
                if isinstance(bulk_job, list) and bulk_job and isinstance(bulk_job[0], list):
                    headers = bulk_job[0]
                    data = bulk_job[1:]
                    results = [dict(zip(headers, row)) for row in data]
                ## Case 2: bulk_job is a list of dicts (already parsed)
                elif isinstance(bulk_job, list) and bulk_job and isinstance(bulk_job[0], dict):
                    results = bulk_job
                else:
                    logger.warning(f"No data found or unexpected bulk response for query: {soql}")
            else:
                raise ValueError(f"Invalid query method: {method}. Use 'rest' or 'bulk'")
            if not results:
                logger.info(f"No data found for query: {soql}")
                return pl.DataFrame()
            # Parse out attributes (Type and URL) and convert to Polars DataFrame
            df = pl.DataFrame(results)
            if 'attributes' in df.columns:
                df = df.with_columns([
                    pl.col('attributes').struct.field('type').alias('sf_type'),
                    pl.col('attributes').struct.field('url').alias('sf_url')
                ]).drop('attributes')
            return df
        except Exception as e:
            logger.exception("Failed to execute Salesforce query")
            raise RuntimeError(f"Failed to execute Salesforce query: {str(e)}")

    def update(self) -> pl.DataFrame:
        """Not implemented."""
        ...

    def delete(self) -> pl.DataFrame:
        """Not implemented."""
        ...

    def _get_sobject_from_query(self, soql: str) -> str:
        match = re.search(r'FROM\s+(\w+)', soql, re.IGNORECASE)
        if not match:
            raise ValueError("Could not determine object from query")
        return match.group(1)



