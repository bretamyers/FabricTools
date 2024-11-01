# Fabric Data Warehouse Query Cost Analyzer


### Description
The Fabric Data Warehouse Query Cost Analyzer is a packaged solution that allows you to easily capture and store the capacity unit seconds (CUs) that a query used. The NB_DW_Load_Cost_Analyzer script allows you to define a data warehouse, a set of queries to be executed, and a concurrency number to specify how many queries you want to execute at once.

The solution will deploy all required artifacts (two Lakehouse, a Data Warehouse, and setup and query capturing notebooks) that includes a sample dataset and queries that will be executed and metrics captured as part of the deployment.

### Prerequisites
1. The [Fabric Capacity Metrics App](https://learn.microsoft.com/en-us/fabric/enterprise/metrics-app-install?tabs=1st) installed and configured.

https://github.com/user-attachments/assets/61895b7e-f0a7-4709-9e02-b82276665261

### How to deploy

1. Create a Fabric workspace and assign either a trial or Fabric capacity to the workspace.
2. Create a notebook artifact within the workspace.
3. Copy and paste the script below into the notebook and run the cell.
```python
import requests, json, base64, time
from notebookutils import mssparkutils

url = "https://raw.githubusercontent.com/bretamyers/FabricTools/main/FabricDWQueryCostAnalyzer/src/NB_Setup_DW_Query_Analyzer.ipynb"
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
            displayHTML(f"""Notebook created (click on link and update parameters in notebook) - <a href="https://app.fabric.microsoft.com/groups/{spark.conf.get('trident.workspace.id')}/synapsenotebooks/{response.json().get('id')}?experience=data-engineering">NB_DW_Load_Cost_Analyzer</a>""")
            break
        else:
            time.sleep(int(response.headers.get('Retry-After')))
else:
    print(f'Error in creating notebook - {response.text}')
```
4. Click on the cells output hyperlink to navigate to the setup notebook.
6. In the setup notebook, update the **CapacityMetricsWorkspace** and **CapacityMetricsDataset** in the first cell to match the capacity metrics workspace and dataset name that you have access to.
7. Click on the "Run all" button in the top toolbar.

After the process has deployed and an execution with the sample dataset has completed, the lakehouse **LH_QueryResults** will contain two tables **QueryResults** and **RunResults**. The **QueryResults** table will contain the individial queries that were executed as part of an execution. The **RunResults** will contain the overall run metrics (an aggregate of the query metrics).

<br></br>

### Solution Artifacts
Name | Type | Description
-|-|-
LH_QueryResults | Lakehouse | A lakehouse that is used to store the query results. It contains two tables.
LH_SampleData | Lakehouse | A lakehouse that contains a sample dataset (Wide World Importers DW)
WH_SampleData | Data Warehouse | A data warehouse that contains a sample dataset (Wide World Importers DW)
NB_DW_Load_Cost_Analyzer | Notebook | A notebook that executes and captures the query results into the LH_QueryResults tables. This is the primary notebook where you can defined the paramters and queries to be used.
NB_Setup_DW_Query_Analyzer | Notebook | A notebook that deploys all the required artifacts to run the sample queries and captures the results.

<br></br>

## Data Dictionary
#### [LH_QueryResults].[dbo].[RunResults]
Column | Description
-|-
CompatibilityLevel | The compatibility level of the warehouse.
DWCreateDate | The creation date of the warehouse.
DWName | The warehouse name.
DWVersion | The warehouse version.
DataLakeLogPublishing | The delta lake log publish setting code used at the start of the run.
DataLakeLogPublishingDesc | The delta lake log publish setting description setting used at the start of the run.
IsVOrderEnabled | The v-order setting of the warehouse at the start of the run.
ServerGuid | The server GUID of the warehouse.
RunStartDateTimeUTC | The start time of the run (UTC).
RunStartTimeEpoch | The start time of the run (EPOCH).
RunId | A GUID for the run.
DWConnectionString | The warehouse connection string to the sql endpoint.
QueriesExecutedCnt | The number of query executed for the run.
RunConcurrency | The concurrency number that was used for the run.
DWGuid | The GUID of the warehouse used.
WorkspaceName | The workspace name that the warehouse resides.
WorkspaceGuid | The workspace GUID.
CapacityName | The Fabric capacity name.
CapacityGuid | The Fabric capacity GUID.
CapacitySKU | The Fabric capacity size that set at the start of the run.
CapacityRegion | The region of the Fabric capacity.
RunEndDatetimeUTC | The end time of the run (UTC).
RunEndTimeEpochMS | The end time of the run (EPOCH).
RunDurationMS | Total run duration in milliseconds.
RunCUSeconds | Total capacuty unit seconds used across all the queries executed within the run.
RunCostPayGo | Pay as you go cost in pennies of all the queries executed. This is what the run would cost if capacity is using pay as you go pricing.
RunCostReserved | Reserved instance cost in pennies of all the queries executed. This is what the run would cost if capacity is using reserved instance pricing.
CapacityDailyCUSeconds | The total amount of capacity unit seconds for a 24 hour period based on the capacity SKU used.
CapacityDailyCostPayGo | Pay as you go total cost over a 24 hour period for the capacity SKU used. This is what the query would cost if running for 24 hours and is using pay as you go pricing.
CapacityDailyCostReserved | Reserved instance total cost over a 24 hour period besed on the capacity SKU used. This is what the capacity would cost if running for 24 hours and is using reserved instance pricing.

<br></br>

#### [LH_QueryResults].[dbo].[QueryResults]
Column | Description
-|-
RunId | The run GUID.
QueryId | A GUID for the query.
QueryStatement | The query statement that was executed.
QueryStartDateTimeUTC | The start time of the query (UTC).
QueryEndDateTimeUTC | The end time of the query (UTC).
ReturnMessage | The full return message of the query.
QueryStartTimeEpochMS | The start time of the query (EPOCH).
QueryEndTimeEpochMS | The end time of the query (EPOCH).
QueryDurationMS | The query duration in milliseconds.
StatementId | The statement Id of the query.
QueryHash | The query hash value.
DistributionRequestId | The distribution request id.
ResultSet | The results returned from the query in json format.
QueryCUSeconds | Total capacity unity seconds of the query.
QueryCostPayGo | Pay as you go cost of the query in pennies. This is what the query would cost if capacity is using pay as you go pricing.
QueryCostReserved | Reserved instance cost of the query in pennies. This is what the query would cost if capacity is using reserved instance pricing.

