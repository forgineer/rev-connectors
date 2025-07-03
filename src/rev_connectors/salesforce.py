import polars as pl
import re
import logging
import requests
import io
import os
from simple_salesforce import Salesforce as SF
from itertools import batched
from . import BaseConnector

logger = logging.getLogger(__name__)


@pl.api.register_dataframe_namespace('salesforce')
class Salesforce(BaseConnector):
    """Connector for Salesforce using simple_salesforce and Polars."""
    def __init__(self, credentials: dict) -> None:
        self.sf = SF(**credentials)
        self.version = self.sf.sf_version
        self.headers = {"Authorization": f"Bearer {self.sf.session_id}", "Content-Type": "application/json"}

    def create(self, 
               sobject: str, 
               data: pl.DataFrame = None, 
               input_file: str = None,
               method: str = 'rest', 
               to_dataframe: bool = True, 
               output_dir: str | None='results',
               upsert_key: str = None, 
               batch_size: int = 200, 
               all_or_none: bool = False) -> pl.DataFrame:
        """Execute a Batched REST or Bulk - Insert or Upsert operation."""
        try:
            # Load data through REST API
            if method == 'rest':
                results = []
                for batch in batched(data, batch_size):
                    # Prepare batched records for load
                    records = [{"attributes": {"type": sobject}, **record} for record in batch]
                    composite_body = {"allOrNone": all_or_none, "records": records}
                    # Upsert or Insert method using REST API
                    if upsert_key:
                        url = f"https://{self.sf.sf_instance}/services/data/v{self.version}/composite/sobjects/{sobject}/{upsert_key}"
                        response = requests.patch(url=url, headers=self.headers, json=composite_body)
                    else:
                        url = f"https://{self.sf.sf_instance}/services/data/v{self.version}/composite/sobjects"
                        response = requests.post(url=url, headers=self.headers, json=composite_body)
                    response.raise_for_status()
                    batch_results = response.json()
                    # Join Fields to results
                    for i, result in enumerate(batch_results):
                        for key, value in batch[i].items():
                            result[key] = value
                    results.extend(batch_results)
                return pl.DataFrame(results)
            
            # Load data through Bulk2 API (Insert or Upsert)
            elif method == 'bulk2':
                if upsert_key:
                    results = getattr(self.sf.bulk2, sobject).upsert(
                        csv_file=input_file, records=data, external_id_field=upsert_key
                    )
                else:
                    results = getattr(self.sf.bulk2, sobject).insert(
                        csv_file=input_file, records=data
                    )
                df = self._result_handler(results, to_dataframe, output_dir, sobject, ingest=True)
                if to_dataframe:
                    return df
        except Exception as e:
            logger.exception("Failed to execute Salesforce create")
            raise RuntimeError(f"Failed to execute Salesforce create: {str(e)}")
        
    def read(self, 
             soql: str, 
             method: str = 'rest', 
             to_dataframe: bool = True, 
             output_dir: str | None='results') -> pl.DataFrame:
        """Execute a SOQL query using REST or Bulk2 API and return a Polars DataFrame."""
        try:
            # Query REST, Parse out attributes (Type and URL) and convert to Polars DataFrame
            if method == 'rest':
                results = self.sf.query_all(soql)['records']
                df = pl.DataFrame(results)
                if 'attributes' in df.columns:
                    df = df.with_columns([
                        pl.col('attributes').struct.field('type').alias('sf_type'),
                        pl.col('attributes').struct.field('url').alias('sf_url')
                    ]).drop('attributes')
                if not results:
                    logger.info(f"No data found for query: {soql}")
                    return pl.DataFrame()
                return df
            
            # Query Bulk2 return as CSV or Dataframe
            elif method == 'bulk2':
                sobject = self._get_sobject_from_query(soql)
                results = getattr(self.sf.bulk2, sobject).query(
                    soql, max_records=50000, column_delimiter="COMMA", line_ending="LF"
                )
                df = self._result_handler(results, to_dataframe, output_dir, sobject, ingest=False)
                if to_dataframe:
                    return df
            else:
                raise ValueError(f"Invalid query method: {method}. Use 'rest' or 'bulk2'")
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
    
    def _result_handler(self, 
                        results, 
                        to_dataframe: bool, 
                        output_dir: str | None,
                        sobject: str,
                        ingest: bool) -> pl.DataFrame:
        """Returns a DataFrame or writes results to CSV files based on the ingest flag."""
        if to_dataframe:
            if ingest:
                for result in results:
                    job_id = result['job_id']
                    # Get successful, failed, and unprocessed records
                    success_df = getattr(self.sf.bulk2, sobject).get_successful_records(job_id)
                    failed_df = getattr(self.sf.bulk2, sobject).get_failed_records(job_id)
                    unprocessed_df = getattr(self.sf.bulk2, sobject).get_unprocessed_records(job_id)

                    # Read CSVs into DataFrames and add a status column
                    success = pl.read_csv(io.StringIO(success_df)).with_columns(pl.lit("success").alias("status"))
                    failed = pl.read_csv(io.StringIO(failed_df)).with_columns(pl.lit("failed").alias("status"))
                    unprocessed = pl.read_csv(io.StringIO(unprocessed_df)).with_columns(pl.lit("unprocessed").alias("status"))

                    df = pl.concat([success, failed, unprocessed], how="diagonal")
                    return df
            else:
                dfs = [pl.read_csv(io.StringIO(csv_str)) for csv_str in results]
                return pl.concat(dfs)
        else:
            if ingest:
                for result in results:
                    job_id = result['job_id']
                    combined_csv = ""
                    status_labels = ["success", "failed", "unprocessed"]

                    # Get successful, failed, and unprocessed records and write to CSV
                    for idx, (csv_str, status) in enumerate(zip([
                        getattr(self.sf.bulk2, sobject).get_successful_records(job_id),
                        getattr(self.sf.bulk2, sobject).get_failed_records(job_id),
                        getattr(self.sf.bulk2, sobject).get_unprocessed_records(job_id)
                    ], status_labels)):
                        if csv_str.strip():
                            lines = csv_str.splitlines()
                            if len(lines) == 0:
                                continue
                            header = lines[0]
                            if idx == 0:
                                combined_csv += f'{header},"status"\n'
                            for row in lines[1:] if idx > 0 else lines[1:]:
                                if row.strip():
                                    combined_csv += f'{row},"{status}"\n'
                    if combined_csv:
                        with open(os.path.join(output_dir, f"{job_id}_combined.csv"), "w") as f:
                            f.write(combined_csv)
            else:
                os.makedirs(output_dir, exist_ok=True)
                for i, data in enumerate(results):
                    with open(os.path.join(output_dir, f"part-{i+1}.csv"), "w") as bos:
                        bos.write(data)





