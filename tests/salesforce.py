import json
import pandas as pd
import polars as pl
import time
from rev_connectors import salesforce

# Temporary sf-api-tools import workaround
# import sys
# import os
# current_dir = os.path.dirname(os.path.abspath(__file__))
# project_root = os.path.dirname(os.path.dirname(os.path.dirname(current_dir)))
# sf_api_tools_path = os.path.join(project_root, 'sf-api-tools\\src')
# sys.path.append(sf_api_tools_path)
# import auth


if __name__ == "__main__":

    # Load credentials
    with open('C:\\Users\\rymil\\Documents\\Misc\\credentials.json', 'r') as f:
        credentials = json.load(f)

    """" Sample credentials.json:
        {
    "login_url": "https://login.salesforce.com/services/Soap/u/60.0",
    "username": "your_username",
    "password": "your_password",
    "security_token": "your_security_token",
    "domain": "login",
    "oauth_key": "your_api_key",
    "oauth_secret": "your_api_secret"
    }
    """

    # Define Query
    data_query = """
        SELECT 
            Id 
            , Name 
        FROM Contact
    """
    
    ##### sf-api-tools REST Connector Query #####
    # start_time = time.time()
    # session = auth.login_password(
    #     login_url=credentials['login_url'],
    #     username=credentials['username'],
    #     password=credentials['password'],
    #     security_token=credentials['security_token']
    # )

    # sf = pd.DataFrame().salesforce
    # sf.set_client(salesforce.SfApiToolsClient(session=session))

    # results = sf.read(input=data_query, 
    #                   method='rest')
    
    # # print(results)
    # print(f"\nsf-api-tools REST Query execution time: {time.time() - start_time:.2f} seconds")
    # print(f"Records returned: {len(results)}")


    ##### simple-salesforce REST Connector Query (Pandas) #####
    start_time = time.time()
    sf = pd.DataFrame().salesforce
    sf.set_client(salesforce.SimpleSalesforceClient(
        credentials={
            'username': credentials['username'],
            'password': credentials['password'],
            'security_token': credentials['security_token'],
            'domain': 'login'
        }
    ))

    results = sf.read(input=data_query, 
                      method='rest')

    print(results)
    print(f"\nsimple-salesforce REST Query (Pandas) execution time: {time.time() - start_time:.2f} seconds")
    print(f"Records returned: {len(results)}")


    ##### simple-salesforce REST Connector Query (Polars) #####
    start_time = time.time()
    sf = pl.DataFrame().salesforce
    sf.set_client(salesforce.SimpleSalesforceClient(
        credentials={
            'username': credentials['username'],
            'password': credentials['password'],
            'security_token': credentials['security_token'],
            'domain': 'login'
        }
    ))

    results = sf.read(input=data_query, 
                      method='rest')

    print(results)
    print(f"\nsimple-salesforce REST Query (Polars) execution time: {time.time() - start_time:.2f} seconds")
    print(f"Records returned: {len(results)}")


    ##### simple-salesforce Bulk Connector Query #####
    start_time = time.time()
    bulk_results = sf.read(input=data_query, method='bulk')
    bulk_results = pl.DataFrame(bulk_results).drop(['attributes'])

    print(bulk_results)
    print(f"\nsimple-salesforce Bulk Query execution time: {time.time() - start_time:.2f} seconds")
    print(f"Records returned: {len(bulk_results)}")
    
