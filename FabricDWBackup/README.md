# Fabric Data Warehouse Backup


### Description
An art of the possible solution to demonstrate how you implemented data backups for a Fabric warehouse and restore the data.



```python
import requests, json, base64, time
from notebookutils import mssparkutils

def get_notebook_definition(notebookName:str):
    url = f"https://raw.githubusercontent.com/bretamyers/FabricTools/main/FabricDWBackup/src/{notebookName}.ipynb"
    response = requests.get(url)
    return response.json()

def create_notebook(notebookName:str):
    notebookDefinition = get_notebook_definition(notebookName)

    header = {'Authorization': f'Bearer {mssparkutils.credentials.getToken("pbi")}', "Content-Type": "application/json"}
    
    body = {
        "displayName": notebookName,
        "definition": {
            "format": "ipynb",
            "parts": [
                {
                    "path": "notebook-content.py"
                    ,"payload": base64.b64encode(json.dumps(notebookDefinition).encode('utf-8')).decode('utf-8')
                    ,"payloadType": "InlineBase64"
                }
            ]
        }
    }

    response = requests.request(method='post', url=f"https://api.fabric.microsoft.com/v1/workspaces/{spark.conf.get('trident.workspace.id')}/notebooks", headers=header, data=json.dumps(body))
        
    if response.status_code == 202:
        print('Notebook is creating...', end='\r')
        for retry in range(5):
            response = requests.request(method='get', url=response.headers.get('Location'), headers=header)
            if response.json().get('status') == 'Succeeded':
                response = requests.request(method='get', url=f"{response.headers.get('Location')}", headers=header)
                print(" "*50) # Clears the previous print statement
                displayHTML(f"""Notebook created (click on link and update parameters in notebook) - <a href="https://app.fabric.microsoft.com/groups/{spark.conf.get('trident.workspace.id')}/synapsenotebooks/{response.json().get('id')}?experience=data-engineering">{notebookName}</a>""")
                break
            else:
                time.sleep(int(response.headers.get('Retry-After')))
    else:
        print(f'Error in creating notebook - {response.text}')


notebookList = ['NB_Backup_DW_To_LH', 'NB_Restore_DW_From_LH']
for notebookName in notebookList:
    create_notebook(notebookName)
```

