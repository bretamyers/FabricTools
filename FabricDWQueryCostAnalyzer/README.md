## How to deploy

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
    "displayName": "NB_Setup_DW_Query_Analyzer_221236",
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
    print('Notebook is creating...')
    for retry in range(5):
        response = requests.request(method='get', url=response.headers.get('Location'), headers=header)
        if response.json().get('status') == 'Succeeded':
            response = requests.request(method='get', url=f"{response.headers.get('Location')}", headers=header)
            displayHTML(f"""<a href="https://app.fabric.microsoft.com/groups/{spark.conf.get('trident.workspace.id')}/synapsenotebooks/{response.json().get('id')}?experience=data-engineering">NB_DW_Load_Cost_Analyzer</a>""")
            break
        else:
            time.sleep(int(response.headers.get('Retry-After')
else:
    print(f'Error in creating notebook - {response.text}')
```
4. Click on the cells output hyperlink.
6. Update the "CapacityMetricsWorkspace" and "CapacityMetricsDataset" in the first cell.
7. Click on the "Run all" button in the top toolbar.