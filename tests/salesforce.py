import unittest
import json
from rev_connectors.salesforce import Salesforce
import polars as pl
import logging
import time


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TestSalesforceConnector(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Load credentials once for all tests
        with open('./tests/credentials.json', 'r') as f:
            credentials = json.load(f)
        cls.sf = Salesforce(
            credentials={
                'username': credentials['salesforce']['username'],
                'password': credentials['salesforce']['password'],
                'security_token': credentials['salesforce']['security_token'],
                'domain': credentials['salesforce']['domain']
            }
        )
        cls.data_query = """
            SELECT 
                Id, Name
            FROM Account
        """

    def test_rest_query_returns_dataframe(self):
        """Test that REST query returns a DataFrame with expected columns."""
        start_time = time.time()
        results = self.sf.read(soql=self.data_query, method='rest')
        elapsed = time.time() - start_time
        self.assertIsInstance(results, pl.DataFrame)
        self.assertIn('Id', results.columns)
        self.assertIn('Name', results.columns)
        logger.info(f"REST Query returned {len(results)} records in {elapsed:.2f} seconds.")
        logger.info("\n%s", results)

    def test_bulk_query_returns_dataframe(self):
        """Test that Bulk query returns a DataFrame with expected columns."""
        start_time = time.time()
        results = self.sf.read(soql=self.data_query, method='bulk')
        elapsed = time.time() - start_time
        self.assertIsInstance(results, pl.DataFrame)
        self.assertIn('Id', results.columns)
        self.assertIn('Name', results.columns)
        logger.info(f"Bulk Query returned {len(results)} records in {elapsed:.2f} seconds.")
        logger.info("\n%s", results)

    def test_rest_query_returns_empty_dataframe(self):
        """Test REST query that should return no results returns an empty DataFrame."""
        empty_query = "SELECT Id FROM Account WHERE Name = 'DefinitelyNotARealName12345'"
        results = self.sf.read(soql=empty_query, method='rest')
        self.assertIsInstance(results, pl.DataFrame)
        self.assertEqual(len(results), 0)
        logger.info("Empty REST query returned 0 records as expected.")

    def test_bulk_query_returns_empty_dataframe(self):
        """Test Bulk query that should return no results returns an empty DataFrame."""
        empty_query = "SELECT Id FROM Account WHERE Name = 'DefinitelyNotARealName12345'"
        results = self.sf.read(soql=empty_query, method='bulk')
        self.assertIsInstance(results, pl.DataFrame)
        self.assertEqual(len(results), 0)
        logger.info("Empty Bulk query returned 0 records as expected.")


if __name__ == "__main__":
    unittest.main()