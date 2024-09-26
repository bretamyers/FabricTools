## How to deploy

1. Create a Fabric workspace and assign either a trial or Fabric capacity to the workspace.
2. Create a notebook artifact within the workspace.
3. Copy and paste the script below into the notebook and run the cell.
```python
import requests, json, base64
from notebookutils import mssparkutils

url = "https://raw.githubusercontent.com/bretamyers/FabricTools/dev/FabricDWQueryCostAnalyzer/src/NB_Setup_DW_Cost_Analyzer.ipynb"
response = requests.get(url)
body = {
    "displayName": "NB_Setup_DW_Cost_Analyzer",
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
response = requests.request(method='post', url=f"https://api.fabric.microsoft.com/v1/workspaces/{spark.conf.get('trident.workspace.id')}/notebooks"
    ,headers={'Authorization': f'Bearer {mssparkutils.credentials.getToken("pbi")}', "Content-Type": "application/json"}
    ,data=json.dumps(body))
print(response.status_code)
```
4. Navigate back to the workspace and do a refresh on the page. 
5. Open the newly created notebook "NB_Setup_DW_Cost_Analyzer".
6. Update the "CapacityMetricsWorkspace" and "CapacityMetricsDataset" in the first cell.
7. Click on the "Run all" button in the top toolbar.