# Fabric Data Warehouse Query Cost Analyzer


#### Description
The Fabric Data Warehouse Query Cost Analyzer is a packaged solution that allows you to easily capture and store the capacity unit seconds (CUs) that a query used. The NB_DW_Load_Cost_Analyzer script allows you to define a data warehouse, a set of queries to be executed, and a concurrency number to specify how many queries you want to execute at once.

The solution will deploy all required artifacts (two Lakehouse, a Data Warehouse, and setup and query capturing notebooks) that includes a sample dataset and queries that will be executed and metrics captured as part of the deployment.


#### Prerequisites
1. The [Fabric Capacity Metrics App](https://learn.microsoft.com/en-us/fabric/enterprise/metrics-app-install?tabs=1st) installed and configured.


#### How to deploy

1. Create a Fabric workspace and assign either a trial or Fabric capacity to the workspace.
2. Create a notebook artifact within the workspace.
3. Copy and paste the script below into the notebook and run the cell.
```python
import requests, json, base64, time
from notebookutils import mssparkutils

url = "https://raw.githubusercontent.com/bretamyers/FabricTools/dev/FabricDWQueryCostAnalyzer/src/NB_Setup_DW_Query_Analyzer.ipynb"
header = {'Authorization': f'Bearer {mssparkutils.credentials.getToken("pbi")}', "Content-Type": "application/json"}
response = requests.get(url)

body = {
    "displayName": "NB_Setup_DW_Query_Analyzer",
    "definition": {
        "format": "ipynb",
        "parts": [
            {
                "path": "notebook-content.py"
                ,"payload": base64.b64encode(json.dumps(response.json()).encode('utf-8')).decode('utf-8')
                ,"payloadType": "InlineBase64"
            }
        ]
    }
}

response = requests.request(method='post', url=f"https://api.fabric.microsoft.com/v1/workspaces/{spark.conf.get('trident.workspace.id')}/notebooks",headers=header, data=json.dumps(body))
    
if response.status_code == 202:
    print('Notebook is creating...', end='\r')
    for retry in range(5):
        response = requests.request(method='get', url=response.headers.get('Location'), headers=header)
        if response.json().get('status') == 'Succeeded':
            response = requests.request(method='get', url=f"{response.headers.get('Location')}", headers=header)
            print(" "*50) # Clears the previous print statement
            displayHTML(f"""<p>Notebook created <a href="https://app.fabric.microsoft.com/groups/{spark.conf.get('trident.workspace.id')}/synapsenotebooks/{response.json().get('id')}?experience=data-engineering">NB_DW_Load_Cost_Analyzer</a></p>""")
            break
        else:
            time.sleep(int(response.headers.get('Retry-After')))
else:
    print(f'Error in creating notebook - {response.text}')
```
4. Click on the cells output hyperlink.
6. Update the "CapacityMetricsWorkspace" and "CapacityMetricsDataset" in the first cell.
7. Click on the "Run all" button in the top toolbar.

#### Solution Artifacts
Name | Type | Description
-|-|-
LH_QueryResults | Lakehouse | A lakehouse that is used to store the query results. It contains two tables.
LH_SampleData | Lakehouse | A lakehouse that contains a sample dataset (Wide World Importers DW)
WH_SampleData | Data Warehouse | A data warehouse that contains a sample dataset (Wide World Importers DW)
NB_DW_Load_Cost_Analyzer | Notebook | A notebook that executes and captures the query results into the LH_QueryResults tables. This is the primary notebook where you can defined the paramters and queries to be used.
NB_Setup_DW_Query_Analyzer | Notebook | A notebook that deploys all the required artifacts to run the sample queries and captures the results.
