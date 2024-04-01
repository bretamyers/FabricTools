import requests, json, logging, time

logger = logging.getLogger(__name__)

class fabric_rest():
    def __init__(self, audience:str='pbi'):
        self.header = self.create_header(audience)


    def create_header(self, audience:str='pbi') -> dict:
        token = 'eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsIng1dCI6IlhSdmtvOFA3QTNVYVdTblU3Yk05blQwTWpoQSIsImtpZCI6IlhSdmtvOFA3QTNVYVdTblU3Yk05blQwTWpoQSJ9.eyJhdWQiOiJodHRwczovL2FuYWx5c2lzLndpbmRvd3MubmV0L3Bvd2VyYmkvYXBpIiwiaXNzIjoiaHR0cHM6Ly9zdHMud2luZG93cy5uZXQvYmM4NmVhM2YtNmIxMS00MDIxLThmOTgtZjQyNmZiYjNiNzE3LyIsImlhdCI6MTcxMTgzNDMzNiwibmJmIjoxNzExODM0MzM2LCJleHAiOjE3MTE4MzkxMTEsImFjY3QiOjAsImFjciI6IjEiLCJhaW8iOiJBVlFBcS84V0FBQUErbEU5eldlL1dkTWR3LzVib05laFNDVWhUcGhLcFdvZ2MwUzhua1ZvWm1GMFVVYm9HQ0tSNlFzSDhzY3UwVVpobzM0TGQvY1RQbzR0KzdSbnlxOHkwNDRCWDNVeVQ2N2JEWnRFME93NFpUOD0iLCJhbXIiOlsicHdkIiwibWZhIl0sImFwcGlkIjoiODcxYzAxMGYtNWU2MS00ZmIxLTgzYWMtOTg2MTBhN2U5MTEwIiwiYXBwaWRhY3IiOiIyIiwiZmFtaWx5X25hbWUiOiJNQ0FQUyIsImdpdmVuX25hbWUiOiJhZG1pbiIsImlwYWRkciI6IjQ3LjIyNS4xOC4xMSIsIm5hbWUiOiJhZG1pbiIsIm9pZCI6IjQ5NmU5ODkzLTBmOTYtNDc4MC1iNDQ0LWQwNmNlNDRmMTdlNSIsInB1aWQiOiIxMDAzMjAwMjY2MTc1QzUzIiwicmgiOiIwLkFWSUFQLXFHdkJGcklVQ1BtUFFtLTdPM0Z3a0FBQUFBQUFBQXdBQUFBQUFBQUFDNkFPSS4iLCJzY3AiOiJ1c2VyX2ltcGVyc29uYXRpb24iLCJzdWIiOiI4ZVVaQ0ZnQTdhWHNMQlZuWkJXQ1NlYndJZVZvcmNVMUZCbU1qQzhiZm5FIiwidGlkIjoiYmM4NmVhM2YtNmIxMS00MDIxLThmOTgtZjQyNmZiYjNiNzE3IiwidW5pcXVlX25hbWUiOiJhZG1pbkBNbmdFbnZNQ0FQODQ2Njg0Lm9ubWljcm9zb2Z0LmNvbSIsInVwbiI6ImFkbWluQE1uZ0Vudk1DQVA4NDY2ODQub25taWNyb3NvZnQuY29tIiwidXRpIjoiSDN2Y2xTaXU3RUdla0dEOEswdUlBQSIsInZlciI6IjEuMCIsIndpZHMiOlsiNjJlOTAzOTQtNjlmNS00MjM3LTkxOTAtMDEyMTc3MTQ1ZTEwIiwiYjc5ZmJmNGQtM2VmOS00Njg5LTgxNDMtNzZiMTk0ZTg1NTA5Il0sInhtc19wbCI6ImVuLVVTIn0.BJV-tVYjOJJk4YXgJJQi1nXeS27NByeASmJtGMKxtFVSe6w8N08h9JSi-RsEzpQ0Wl7rUq5VaN1LYOafpxBVw5ZyUek4WEE-Xzb_DH_8h5KxAPB5tRSEwv7u3ZpLpvmrRZhYkjvJBQubgnVEd74UEWzCGGadls9PKGDZTpvTVHMHbJjUOfX4oMiR6WU_yIK8kWA45ymrl7zSNOv1iP6coDEULDv4w9ez7uTChPZbHD6q-FoM-6HsOUv0Ig-e67sZFJIZ_8iIvCO3Qbq2ANQUNL6ILeXD0ID124cJnChGvblsejmoML1SpQqaNwOMMG3qIQJlrmT4DX8fOmrBcvvXMg'
        return {'Authorization': f'Bearer {token}', 'Content-type': 'application/json'}


    def call_rest(self, method:str, url:str, body:dict=None) -> requests.Response:
        try:
            response = requests.request(method=method, url=url, headers=self.header, data=json.dumps(body))
            print(response.json())
            response.raise_for_status()
            logger.info(f"Success - {response}")
            if response.status_code == 202:
                response = self.response_long_running(response=response)
            return response
        except requests.exceptions.RequestException as err:
            print ("Error", err.response.text)
        except requests.exceptions.HTTPError as errh:
            print ("Http Error:", errh.response.text)
        except requests.exceptions.ConnectionError as errc:
            print ("Error Connecting:", errc.response.text)
        except requests.exceptions.Timeout as errt:
            print ("Timeout Error:", errt.response.text)


    def response_long_running(self, response:requests.Response) -> requests.Response:
        responseLocation = response.headers.get('Location')
        for _ in range(5):
            responseStatus = self.call_rest(method='get', url=responseLocation)
            if responseStatus.json().get('status') != 'Succeeded':
                logger.info(f'Operation {response.headers.get("x-ms-operation-id")} is not ready. Waiting for {response.headers.get("Retry-After")} seconds.')
                time.sleep(int(response.headers.get('Retry-After')))
            else:
                logger.info('Payload is ready. Requesting the result.')
                responseResult = self.call_rest(method='get', url=f'{responseLocation}/result')
                return responseResult


    # https://learn.microsoft.com/en-us/rest/api/fabric/admin/workspaces/list-workspaces?tabs=HTTP
    def workspace_get_id(self, workspaceName:str) -> str:
        response = self.call_rest(method='get', url='https://api.fabric.microsoft.com/v1/workspaces')
        workspaceId = [workspace.get('id') for workspace in response.json().get('value') if workspace.get('displayName') == workspaceName][0]
        return workspaceId


    def pipeline_list_response(self, workspaceName:str) -> str:
        pipelineResponse = self.item_get_response(workspaceName=workspaceName, itemType='DataPipeline')
        return pipelineResponse
    

    def pipeline_get_object(self, workspaceName:str, pipelineName:str) -> str:
        lakehouseId = self.item_get_object(workspaceName=workspaceName, itemName=pipelineName, itemType='DataPipeline')
        return lakehouseId
    

    # https://learn.microsoft.com/en-us/rest/api/fabric/admin/items/list-items?tabs=HTTP
    def pipeline_get_id(self, workspaceName:str, pipelineName:str) -> str:
        pipelineId = self.item_get_id(workspaceName=workspaceName, itemName=pipelineName, itemType='DataPipeline')
        return pipelineId


    # https://learn.microsoft.com/en-us/rest/api/fabric/core/items/get-item-definition?tabs=HTTP
    def pipeline_get_definition_parts(self, workspaceName:str, pipelineName:str) -> list:
        itemDefinitionParts = self.item_get_definition_parts(workspaceName=workspaceName, itemName=pipelineName, itemType='DataPipeline')
        return itemDefinitionParts


    # https://learn.microsoft.com/en-us/rest/api/fabric/core/items/create-item?tabs=HTTP
    def pipeline_clone(self, workspaceNameSource:str, pipelineNameSource:str, workspaceNameTarget:str, pipelineNameTarget:str) -> requests.Response:
        pipelinePartsList = self.pipeline_get_definition_parts(workspaceName=workspaceNameSource, pipelineName=pipelineNameSource)
        body = {"displayName": pipelineNameTarget
                ,"type": "DataPipeline"
                ,"definition": {
                    "parts": pipelinePartsList
                    }
                }

        workspaceId = self.workspace_get_id(workspaceName=workspaceNameTarget)
        
        response = self.call_rest(method='post', url=f'https://api.fabric.microsoft.com/v1/workspaces/{workspaceId}/items', body=body)
        return response

    # https://learn.microsoft.com/en-us/rest/api/fabric/admin/items/list-items?tabs=HTTP
    def notebook_get_id(self, workspaceName:str, notebookName:str) -> str:
        workspaceId = self.workspace_get_id(workspaceName=workspaceName)
        response = self.call_rest(method='get', url=f'https://api.fabric.microsoft.com/v1/workspaces/{workspaceId}/items?type=Notebook')
        notebookId = [notebook.get('id') for notebook in response.json().get('value') if notebook.get('displayName') == notebookName][0]
        return notebookId


    def notebook_get_item_definition(self, workspaceName:str, notebookName:str) -> requests.Response:
        workspaceId = self.workspace_get_id(workspaceName=workspaceName)
        notebookId = self.notebook_get_id(workspaceName=workspaceName, notebookName=notebookName)
        response = self.call_rest(method='post', url=f'https://api.fabric.microsoft.com/v1/workspaces/{workspaceId}/items/{notebookId}/getDefinition?format=ipynb')
        definition = response.json().get('definition')
        return definition


    def notebook_clone(self, workspaceNameSource:str, notebookNameSource:str, workspaceNameTarget:str, notebookNameTarget:str) -> requests.Response:
        notebook_get_definition = self.notebook_get_item_definition(workspaceName=workspaceNameSource, notebookName=notebookNameSource).get('definition')
        body = {
            "displayName": notebookNameTarget
            ,"type": "Notebook"
            ,"definition": notebook_get_definition
        }
        workspaceIdTarget = self.workspace_get_id(workspaceName=workspaceNameTarget)
        response = self.call_rest(method='post', url=f'https://api.fabric.microsoft.com/v1/workspaces/{workspaceIdTarget}/items', body=body)
        return response
    

    # https://learn.microsoft.com/en-us/rest/api/fabric/admin/items/list-items?tabs=HTTP
    def item_get_response(self, workspaceName:str, itemType:str='') -> requests.Response:
        workspaceId = self.workspace_get_id(workspaceName=workspaceName)
        itemTypeFilter = f'type={itemType}' if itemType else ''
        response = self.call_rest(method='get', url=f'https://api.fabric.microsoft.com/v1/workspaces/{workspaceId}/items?{itemTypeFilter}')
        return response


    def item_list(self, workspaceName:str, itemType:str='') -> list:
        item_get_response = self.item_get_response(workspaceName=workspaceName, itemType=itemType)
        item_list = item_get_response.json().get('value')
        return item_list


    def item_get_object(self, workspaceName:str, itemName:str, itemType:str='') -> dict:
        item_list = self.item_list(workspaceName=workspaceName, itemType=itemType)
        artifact = [item for item in item_list if item.get('displayName') == itemName][0]
        return artifact


    def item_get_id(self, workspaceName:str, itemName:str, itemType:str='') -> str:
        artifactObject = self.item_get_object(workspaceName=workspaceName, itemName=itemName, itemType=itemType)
        artifactId = artifactObject.get('id')
        return artifactId


    def item_get_definition_response(self, workspaceName:str, itemName:str, itemType:str='') -> requests.Response:
        workspaceId = self.workspace_get_id(workspaceName=workspaceName)
        itemId = self.item_get_id(workspaceName=workspaceName, itemName=itemName, itemType=itemType)
        # logger.info(f'item_get_definition_response {workspaceName=}:{workspaceId=} - {itemName=}:{itemId}')
        response = self.call_rest(method='post', url=f'https://api.fabric.microsoft.com/v1/workspaces/{workspaceId}/items/{itemId}/getDefinition')
        return response
    

    def item_get_definition_parts(self, workspaceName:str, itemName:str, itemType:str='') -> requests.Response:
        response = self.item_get_definition_response(workspaceName=workspaceName, itemName=itemName, itemType=itemType).json().get('definition').get('parts')
        return response


    def item_create(self, workspaceName:str, itemName:str, itemType:str, itemDefinition:dict=None) -> requests.Response:
        workspaceId = self.workspace_get_id(workspaceName=workspaceName)
        body = {"displayName": itemName
                ,"type": itemType
                ,**({ 'itemDefinition': itemDefinition } if itemDefinition is not None else {})
                }
        logger.info(f'item_create - {body=}')
        response = self.call_rest(method='post', url=f'https://api.fabric.microsoft.com/v1/workspaces/{workspaceId}/items', body=body)
        return response
    

    # This one does not work yet!! (for lakehouse)
    def item_delete(self, workspaceName:str, itemName:str):
        # https://wabi-west-us3-a-primary-redirect.analysis.windows.net/metadata/artifacts/0106bc1e-ab38-4a85-9569-7b9100799147
        workspaceId = self.workspace_get_id(workspaceName=workspaceName)
        itemId = self.item_get_id(workspaceName=workspaceName, itemName=itemName)
        logger.info(f'item_delete - {workspaceName=}:{workspaceId=} - {itemName=}:{itemId}')
        url = f'https://api.fabric.microsoft.com/v1/workspaces/{workspaceId}/items/{itemId}'
        response = self.call_rest(method='delete', url=f'https://api.fabric.microsoft.com/v1/workspaces/{workspaceId}/items/{itemId}')
        return response


    def lakehouse_get_shortcut(self, workspaceName:str, itemName:str, shortcutPath:str, shortcutName:str) -> requests.Response:
        workspaceId = self.workspace_get_id(workspaceName=workspaceName)
        itemId = self.artifact_get_id(workspaceName=workspaceName, artifactName=itemName)
        # https://learn.microsoft.com/en-us/rest/api/fabric/core/onelake-shortcuts/get-shortcut?tabs=HTTP
        response = self.call_rest(method='get', url=f'https://api.fabric.microsoft.com/v1/workspaces/{workspaceId}/items/{itemId}/shortcuts/{shortcutPath}/{shortcutName}')
        return response
    

    def _lakehouse_create_shortcut(self, workspaceId:str, itemId:str, body:dict) -> requests.Response:
        response = self.call_rest(method='post', url=f'https://api.fabric.microsoft.com/v1/workspaces/{workspaceId}/items/{itemId}/shortcuts', body=body)
        return response


    # TODO
    def lakehouse_create_shortcut_adls(self, workspaceName:str, itemName:str, shortcutName:str, shortcutPath:str, adlsPath:str, adlsSubPath:str) -> requests.Response:
        workspaceId = self.workspace_get_id(workspaceName=workspaceName)
        itemId = self.artifact_get_id(workspaceName=workspaceName, artifactName=itemName)
        connectionId = ''
        # https://learn.microsoft.com/en-us/rest/api/fabric/core/onelake-shortcuts/create-shortcut?tabs=HTTP
        body = {"path": shortcutPath,
                "name": shortcutName,
                "target": {
                    "adlsGen2": {
                    "location": adlsPath,
                    "subpath": adlsSubPath,
                    "connectionId": connectionId
                    }
                }
            }
        response = self._lakehouse_create_shortcut(workspaceId=workspaceId, itemId=itemId, body=body)
        return response


    # https://learn.microsoft.com/en-us/rest/api/fabric/core/onelake-shortcuts/create-shortcut?tabs=HTTP#create-shortcut-one-lake-target
    def lakehouse_create_shortcut_onelake(self, workspaceName:str, itemName:str, shortcutName:str, shortcutPath:str, onelakePath:str) -> requests.Response:
        workspaceId = self.workspace_get_id(workspaceName=workspaceName)
        itemId = self.artifact_get_id(workspaceName=workspaceName, artifactName=itemName)
        itemId = self.lakehouse_get_shortcut(workspaceName=workspaceName, artifactName=itemName)
        # https://learn.microsoft.com/en-us/rest/api/fabric/core/onelake-shortcuts/create-shortcut?tabs=HTTP
        body = {"path": shortcutPath,
                "name": shortcutName,
                "target": {
                    "oneLake": {
                    "workspaceId": workspaceId,
                    "itemId": itemId,
                    "path": onelakePath
                    }
                }
            }
        response = self._lakehouse_create_shortcut(workspaceId=workspaceId, itemId=itemId, body=body)
        return response

    def connections_response(self, connectionName:str) -> requests.Response:
        response = self.call_rest(method='get', url='https://api.powerbi.com/v2.0/myorg/me/gatewayClusterDatasources') #?$expand=users
        return response


    def connections_list(self, connectionName:str) -> list:
        connections_list = self.connections_response(connectionName=connectionName).json().get('value')
        return connections_list
    

    def connections_get_object(self, connectionName:str) -> dict:
        connectionId = [connection for connection in self.connections_list(connectionName=connectionName) if connection.get('datasourceName') == connectionName][0]
        return connectionId
    
    
    def connections_get_id(self, connectionName:str) -> str:
        connectionId = self.connections_get_object(connectionName=connectionName).get('id')
        return connectionId
    

    def lakehouse_list_response(self, workspaceName:str) -> requests.Response:
        lakehouseResponse = self.item_get_response(workspaceName=workspaceName, itemType='Lakehouse')
        return lakehouseResponse
    

    def lakehouse_get_object(self, workspaceName:str, lakehouseName:str) -> str:
        lakehouseId = self.item_get_object(workspaceName=workspaceName, itemName=lakehouseName, itemType='Lakehouse')
        return lakehouseId
    

    def lakehouse_get_id(self, workspaceName:str, lakehouseName:str) -> str:
        lakehouseId = self.item_get_id(workspaceName=workspaceName, itemName=lakehouseName, itemType='Lakehouse')
        return lakehouseId
    

    # Getting a 403 error 
    def lakehouse_get_definition_response(self, workspaceName:str, lakehouseName:str) -> str:
        lakehouseDefinitionParts = self.item_get_definition_response(workspaceName=workspaceName, itemName=lakehouseName, itemType='Lakehouse')
        return lakehouseDefinitionParts
    
    # Getting a 403 error
    def lakehouse_get_definition_parts(self, workspaceName:str, lakehouseName:str) -> str:
        lakehouseDefinitionParts = self.item_get_definition_parts(workspaceName=workspaceName, itemName=lakehouseName, itemType='Lakehouse')
        return lakehouseDefinitionParts
    

    def lakehouse_create(self, workspaceName:str, lakehouseName:str) -> str:
        lakehouseDefinitionParts = self.item_create(workspaceName=workspaceName, itemName=lakehouseName, itemType='Lakehouse', itemDefinition=None)
        return lakehouseDefinitionParts

    # Is not supported yet
    def lakehouse_delete(self, workspaceName:str, lakehouseName:str) -> str:
        response = self.item_delete(workspaceName=workspaceName, itemName=lakehouseName)
        return response

if __name__ == '__main__':
    
    logger.addHandler(logging.StreamHandler())
    logger.setLevel(logging.INFO)

    # print(fabric_rest().item_list(workspaceName='WS_Steve'))

    # print(fabric_rest().lakehouse_create_shortcut_adls(workspaceName='WS_Steve', itemName='', shortcutName='', shortcutPath='', adlsPath='', adlsSubPath=''))
    
    # print(fabric_rest().lakehouse_get_id(workspaceName='WS_Steve', lakehouseName='LH_InternetSales'))
    # print(fabric_rest().lakehouse_get_object(workspaceName='WS_Steve', lakehouseName='LH_InternetSales'))
    # print(fabric_rest().lakehouse_get_definition_response(workspaceName='WS_Steve', lakehouseName='LH_InternetSales'))
    # print(fabric_rest().lakehouse_get_definition_parts(workspaceName='WS_Steve', lakehouseName='LH_InternetSales'))
    # print(fabric_rest().lakehouse_create(workspaceName='WS_Steve', lakehouseName='LH_Test2'))
    # print(fabric_rest().lakehouse_delete(workspaceName='WS_Steve', lakehouseName='LH_Test2'))
    workspaceId = '372dfc2d-e201-49d7-a28b-7cfc015a9317'
    itemId = 'b5dc0a83-ed62-463f-a9e3-79faba442a6c'
    response = requests.delete(url=f'https://api.fabric.microsoft.com/v1/workspaces/{workspaceId}/items/{itemId}', headers=fabric_rest().header)
    print(response.status_code)
    print(response.json())
    print(response.headers)
    # Pipelines
    # print(fabric_rest().pipeline_get_definition_parts(workspaceName='WS_Steve', pipelineName='PL_Simple'))
    # print(fabric_rest().pipeline_clone(workspaceNameSource='WS_Steve', pipelineNameSource='PL_Simple', workspaceNameTarget='WS_Steve', pipelineNameTarget='PL_Simple5'))
    