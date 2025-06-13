import json
import polars as pl
import time
from rev_connectors import salesforce


if __name__ == "__main__":

    """" Sample credentials.json:
        {
            "salesforce": {
                "username": "your_username",
                "password": "your_password",
                "security_token": "your_security_token",
                "domain": "login",
                "oauth_key": "your_api_key",
                "oauth_secret": "your_api_secret"
            }
        }
    """

    # Load credentials from JSON file
    with open('./tests/credentials.json', 'r') as f:
        credentials = json.load(f)

    # Define Query
    data_query = """
        SELECT 
            Id 
            , Name 
        FROM Contact
    """

    # Initialize Salesforce client using simple-salesforce client adapter
    sf = pl.DataFrame().salesforce
    sf.set_client(salesforce.SimpleSalesforceClient(
        credentials={
            'username': credentials['salesforce']['username'],
            'password': credentials['salesforce']['password'],
            'security_token': credentials['salesforce']['security_token'],
            'domain': credentials['salesforce']['domain']
        }
    ))
    
    ##### simple-salesforce REST Query (Polars) #####
    start_time = time.time()

    results = sf.read(input=data_query, 
                      method='rest')
    
    print(results)
    print(f"\nsimple-salesforce REST Query (Polars) execution time: {time.time() - start_time:.2f} seconds")
    print(f"Records returned: {len(results)}")


    ##### simple-salesforce Bulk Query #####
    start_time = time.time()

    bulk_results = sf.read(input=data_query, 
                           method='bulk')
  
    print(bulk_results)
    print(f"\nsimple-salesforce Bulk Query execution time: {time.time() - start_time:.2f} seconds")
    print(f"Records returned: {len(bulk_results)}")
    
