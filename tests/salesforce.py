import unittest
import json
from rev_connectors.salesforce import Salesforce
import polars as pl
import logging
import time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TestSalesforceCreate(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Load parameters once for all tests
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
        cls.data_1 = [
            {"Name": "Test-Account-1", "My_Ext_Field__c": "Test-Account-1"},
            {"Name": "Test-Account-2", "My_Ext_Field__c": "Test-Account-2"},
            {"Name": "Test-Account-3", "My_Ext_Field__c": "Test-Account-3"},
            {"Name": "Test-Account-4", "My_Ext_Field__c": "Test-Account-4"},
        ]
        cls.data_2 = [
            {"Name": "Test-Account-5", "My_Ext_Field__c": "Test-Account-5"},
            {"Name": "Test-Account-6", "My_Ext_Field__c": "Test-Account-6"},
            {"Name": "Test-Account-7", "My_Ext_Field__c": "Test-Account-7"},
            {"Name": "Test-Account-8", "My_Ext_Field__c": "Test-Account-8"},
        ]
        cls.data_3 = [
            {"Name": "Test-Account-9", "My_Ext_Field__c": "Test-Account-9"},
            {"Name": "Test-Account-10", "My_Ext_Field__c": "Test-Account-10"},
            {"Name": "Test-Account-11", "My_Ext_Field__c": "Test-Account-11"},
            {"Name": "Test-Account-12", "My_Ext_Field__c": "Test-Account-12"},
        ]
        cls.data_4 = [
            {"Name": "Test-Account-13", "My_Ext_Field__c": "Test-Account-13"},
            {"Name": "Test-Account-14", "My_Ext_Field__c": "Test-Account-14"},
            {"Name": "Test-Account-15", "My_Ext_Field__c": "Test-Account-15"},
            {"Name": "Test-Account-16", "My_Ext_Field__c": "Test-Account-16"},
        ]

    def test_create_accounts_batched_rest_upsert(self):
        """Test creating accounts using the REST API Upsert."""
        start_time = time.time()
        results = self.sf.create(sobject='Account',
                                 data=self.data_1, 
                                 method='rest', 
                                 upsert_key='My_Ext_Field__c', 
                                 batch_size=2)
        elapsed = time.time() - start_time
        self.assertIsInstance(results, pl.DataFrame)
        self.assertTrue(results['success'].all())
        logger.info(f"REST Create (Upsert) returned {len(results)} records in {elapsed:.2f} seconds.")
        logger.info("\n%s", results)

    def test_create_accounts_batched_rest_insert(self):
        """Test creating accounts using the REST API Insert."""
        start_time = time.time()
        results = self.sf.create(sobject='Account',
                                 data=self.data_2, 
                                 method='rest', 
                                 batch_size=2)
        elapsed = time.time() - start_time
        self.assertIsInstance(results, pl.DataFrame)
        self.assertTrue(results['success'].all())
        logger.info(f"REST Create (Insert) returned {len(results)} records in {elapsed:.2f} seconds.")
        logger.info("\n%s", results)

    def test_create_accounts_bulk_upsert_df(self):
        """Test creating accounts using the Bulk API Upsert."""
        start_time = time.time()
        results = self.sf.create(sobject='Account',
                                 data=self.data_3, 
                                 method='bulk2',
                                 upsert_key='My_Ext_Field__c',
                                 to_dataframe=True)
        elapsed = time.time() - start_time
        self.assertIsInstance(results, pl.DataFrame)
        self.assertTrue((results["status"] == "success").all())
        logger.info(f"Bulk Create (Upsert) returned {len(results)} records in {elapsed:.2f} seconds.")
        logger.info("\n%s", results)

    def test_create_accounts_bulk_insert_df(self):
        """Test creating accounts using the Bulk API Insert."""
        start_time = time.time()
        results = self.sf.create(sobject='Account',
                                 data=self.data_4, 
                                 method='bulk2',
                                 to_dataframe=True)
        elapsed = time.time() - start_time
        self.assertIsInstance(results, pl.DataFrame)
        self.assertTrue((results["status"] == "success").all())
        logger.info(f"Bulk Create (Upsert) returned {len(results)} records in {elapsed:.2f} seconds.")
        logger.info("\n%s", results)

    # def test_create_accounts_bulk_insert_csv(self):
    #     """Test creating accounts using the Bulk API Insert (CSV Input & CSV Output)."""
    #     self.sf.create(sobject='Account',
    #                    input_file='C:\\Users\\rymil\\Downloads\\mock_accounts - MOCK_DATA(1).csv',
    #                    output_dir='C:\\Users\\rymil\\Desktop\\New folder',
    #                    method='bulk2',
    #                    to_dataframe=False
    #                    )


class TestSalesforceRead(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Load parameters once for all tests
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
            WHERE Name LIKE 'Test-Account%'
        """

    def test_rest_query_returns_dataframe(self):
        """Test that REST query returns a DataFrame with expected columns."""
        start_time = time.time()
        results = self.sf.read(soql=self.data_query, 
                               method='rest')
        elapsed = time.time() - start_time
        self.assertIsInstance(results, pl.DataFrame)
        self.assertIn('Id', results.columns)
        self.assertIn('Name', results.columns)
        logger.info(f"REST Query returned {len(results)} records in {elapsed:.2f} seconds.")
        logger.info("\n%s", results)

    def test_rest_query_returns_empty_dataframe(self):
        """Test REST query that should return no results returns an empty DataFrame."""
        results = self.sf.read(soql="SELECT Id FROM Account WHERE Name = 'DefinitelyNotARealName12345'", 
                               method='rest')
        self.assertIsInstance(results, pl.DataFrame)
        self.assertEqual(len(results), 0)
        logger.info("Empty REST query returned 0 records as expected.")

    def test_bulk_query_returns_dataframe(self):
        """Test that Bulk query returns a DataFrame with expected columns."""
        start_time = time.time()
        results = self.sf.read(soql=self.data_query, 
                               method='bulk2', 
                               to_dataframe=True)
        elapsed = time.time() - start_time
        self.assertIsInstance(results, pl.DataFrame)
        self.assertIn('Id', results.columns)
        self.assertIn('Name', results.columns)
        logger.info(f"Bulk Query returned {len(results)} records in {elapsed:.2f} seconds.")
        logger.info("\n%s", results)

    def test_bulk_query_returns_empty_dataframe(self):
        """Test Bulk query that should return no results returns an empty DataFrame."""
        results = self.sf.read(soql="SELECT Id FROM Account WHERE Name = 'DefinitelyNotARealName12345'", 
                               method='bulk2')
        self.assertIsInstance(results, pl.DataFrame)
        self.assertEqual(len(results), 0)
        logger.info("Empty Bulk query returned 0 records as expected.")

    def test_bulk_query_returns_csv(self):
        """Test that Bulk query returns a CSV"""
        self.sf.read(soql=self.data_query, method='bulk2', to_dataframe=False)

if __name__ == "__main__":
    
    suite = unittest.TestSuite()
    suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(TestSalesforceCreate))
    suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(TestSalesforceRead))
    runner = unittest.TextTestRunner()
    runner.run(suite)


