# Fabric Data Warehouse Backup Solution


### Description
An art of the possible solution to demonstrate how you implemented data backups for a Fabric warehouse and restore the data. Currently, warehouse system-created restore points are taken automatically for you every 8 hours. Users also have the ability to create-user defined restore points to create ad-hoc restore points of your warehouse. These restore points are extremely valuable but are only available for in-place warehouse restores. If a user or process accidentally deletes the warehouse, you lose the ability to restore the warehouse from the restore points taken. This solution aims to help with this limitation today by allowing you to easily schedule data backups from your warehouse and restore those data backups. 


### How it works
#### NB_Backup_DW_To_LH
- This notebook will scan the onelake location of the target warehouse to get all the tables in the warehouse. 
- Take that list and rewrites the data in the lakehouse LH_DW_Backups in delta format landing following the pattern {workspace_name}/{warehouse name}/{yyyyMMdd_HHmm}/{schema_name}/{table_name}.


#### NB_Restore_DW_From_LH
> [!IMPORTANT]
> The warehouse should exist and the sql database project should have been deployed prior to running this notebook so that the tables are present.
- This notebook will scan the onelake location of the target warehouse restore datetime location to get a list of all the tables to be restored.
- Create a temporary lakehouse in the workspace where the target warehouse exists with the naming convention "LH_temp_retore_{warehouse_name}_{backup_datetime}".
- Create OneLake *Table* shortcuts for each table that is being restored with the naming convention "{schema_name}_{table_name}".
- Loop through each table and execute the sql statements that truncates the target table first and then performs a INSERT INTO statement to move data from the lakehouse table shortcut to the warehouse table.


### Prerequisites
A workspace where you have *contributor* or higher permissions.


### How to deploy
1. Open a workspace where you have *contributor* or higher permissions. Ideally, this should be a different workspace than where the warehouse is backed up.
2. Create a notebook within the workspace and copy and paste the python code below into a cell within the notebook.
3. Run the notebook. This will create a lakehouse within the workspace call "LH_DW_Backups" and two notebooks, one for taking data backups of the warehouse tables and another for restoring the data.
4. Schedule the notebook "NB_Backup_DW_To_LH" to run on a time period that meets your requirements e.g. once a day or once a week.


```python
import requests, json, base64, time
from notebookutils import mssparkutils

header = {'Authorization': f'Bearer {mssparkutils.credentials.getToken("pbi")}', "Content-Type": "application/json"}    

workspaceId = spark.conf.get('trident.workspace.id')

def get_notebook_definition(notebookName:str):
    url = f"https://raw.githubusercontent.com/bretamyers/FabricTools/main/FabricDWBackup/src/{notebookName}.ipynb"
    response = requests.get(url)
    return response.json()


def create_lakehouse_backups(lakehouseBackupName:str):
    body = {
        "displayName": lakehouseBackupName
    }
    
    response = requests.request(method='post', url=f'https://api.fabric.microsoft.com/v1/workspaces/{workspaceId}/lakehouses', headers=header, data=json.dumps(body))
    if response.status_code in [201, 202]:
        return None
    else:
        print(f'{response.status_code} - {response.text}')
    

def create_notebook(notebookName:str, lakehouseBackupName:str):

    notebookDefinition = get_notebook_definition(notebookName)

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

    response = requests.request(method='post', url=f"https://api.fabric.microsoft.com/v1/workspaces/{workspaceId}/notebooks", headers=header, data=json.dumps(body))
        
    if response.status_code == 202:
        print(f'Notebook "{notebookName}" is creating...', end='\r')
        for retry in range(5):
            response = requests.request(method='get', url=response.headers.get('Location'), headers=header)
            if response.json().get('status') == 'Succeeded':
                response = requests.request(method='get', url=f"{response.headers.get('Location')}", headers=header)
                print(" "*50) # Clears the previous print statement
                displayHTML(f"""Notebook created (click on link and update parameters in notebook) - <a href="https://app.fabric.microsoft.com/groups/{workspaceId}/synapsenotebooks/{response.json().get('id')}?experience=data-engineering">{notebookName}</a>""")
                break
            else:
                time.sleep(int(response.headers.get('Retry-After')))
    else:
        print(f'Error in creating notebook - {response.text}')

lakehouseBackupName = 'LH_DW_Backups'
create_lakehouse_backups(lakehouseBackupName)

notebookList = ['NB_Backup_DW_To_LH', 'NB_Restore_DW_From_LH']
for notebookName in notebookList:
    create_notebook(notebookName, lakehouseBackupName)
```

