import requests, json, logging, time, datetime, math

logger = logging.getLogger(__name__)

class fabric_rest():
    def __init__(self, audience:str='pbi'):
        self.header = self.create_header(audience)


    def create_header(self, audience:str='pbi') -> dict:
        import _util
        token = _util._get_token_cached()
        return {'Authorization': f'Bearer {token}', 'Content-type': 'application/json'}


    def request(self, method:str, url:str, body:dict=None) -> requests.Response:
        try:
            response = requests.request(method=method, url=url, headers=self.header, data=json.dumps(body))
            logger.debug(response.json())
            response.raise_for_status()
            logger.debug(f"Response - {response.status_code}")
            if response.status_code == 202:
                response = self.response_long_running(response=response)
            return response
        except requests.exceptions.HTTPError as errh:
            print("Http Error:", errh.response.text)
            ## Add a step to check if the error is due to throttling and wait until the restriction is lifted
            if 'Request is blocked by the upstream service until:' in errh.response.json()['message']:
                blockedDatetime = datetime.datetime.strptime(errh.response.json()['message'].split('Request is blocked by the upstream service until: ')[1], '%m/%d/%Y %I:%M:%S %p')
                sleepDuration = math.ceil((blockedDatetime - datetime.datetime.now(datetime.UTC).replace(tzinfo=None)).total_seconds())
                logger.info(f"Sleeping for {sleepDuration} seconds")
                time.sleep(sleepDuration) # pause until we can make the request again
                return self.request(method=method, url=url, body=body)
        except requests.exceptions.ConnectionError as errc:
            print("Error Connecting:", errc.response.text)
        except requests.exceptions.Timeout as errt:
            print("Timeout Error:", errt.response.text)
        except requests.exceptions.RequestException as err:
            print(response.status_code)
            # print("Error", err.response.text)


    def response_long_running(self, response:requests.Response) -> requests.Response:
        responseLocation = response.headers.get('Location')
        for _ in range(5):
            responseStatus = self.request(method='get', url=responseLocation)
            if responseStatus.json().get('status') != 'Succeeded':
                logger.info(f'Operation {response.headers.get("x-ms-operation-id")} is not ready. Waiting for {response.headers.get("Retry-After")} seconds.')
                time.sleep(int(response.headers.get('Retry-After')))
            else:
                logger.info('Payload is ready. Requesting the result.')
                responseResult = self.request(method='get', url=f'{responseLocation}/result')
                return responseResult


    ## Unoffical API
    def capacity_list_response(self) -> requests.Response:
        response = self.request(method='get', url='https://wabi-west-us3-a-primary-redirect.analysis.windows.net/capacities/listbyrollouts')
        return response
    

    def capacity_list(self) -> list:
        capacityList = self.capacity_list_response().json().get('capacitiesMetadata')
        return capacityList
    

    def capacity_get(self, capacityName:str) -> dict:
        capacity = [capacity for capacity in self.capacity_list() if capacity.get('configuration').get('displayName') == capacityName][0]
        return capacity
    

    def workspace_list_response(self) -> requests.Response:
        logger.info('workspace_list_response')
        response = self.request(method='get', url='https://api.fabric.microsoft.com/v1/workspaces')
        return response
    

    def workspace_list(self) -> list:
        logger.info('workspace_list')
        workspaceList = self.workspace_list_response().json().get('value')
        return workspaceList


    # https://learn.microsoft.com/en-us/rest/api/fabric/admin/workspaces/list-workspaces?tabs=HTTP
    def workspace_get_id(self, workspaceName:str) -> str:
        logger.info('workspace_get_id')
        workspaceId = [workspace.get('id') for workspace in self.workspace_list() if workspace.get('displayName') == workspaceName][0]
        return workspaceId
    

    def workspace_get_access_details_response(self, workspaceName:str) -> requests.Response:
        workspaceId = self.workspace_get_id(workspaceName=workspaceName)
        response = self.request(method='get', url=f'https://api.fabric.microsoft.com/v1/admin/workspaces/{workspaceId}/users')
        return response
    

    def _workspace_get_access_details_response_workspace_id(self, workspaceId:str) -> requests.Response:
        response = self.request(method='get', url=f'https://api.fabric.microsoft.com/v1/admin/workspaces/{workspaceId}/users')
        return response


    def workspace_get_access_details(self, workspaceName:str) -> list:
        workspaceAccessDetail = self.workspace_get_access_details_response(workspaceName=workspaceName).json().get('accessDetails')
        return workspaceAccessDetail
    

    def _workspace_get_access_details_workspace_id(self, workspaceId:str) -> list:
        workspaceAccessDetail = self._workspace_get_access_details_response_workspace_id(workspaceId=workspaceId).json().get('accessDetails')
        return workspaceAccessDetail


    # TODO - Need to check if the output makes sense
    # Might change from principal displayName to principal userDetails userPrincipalName. This field is not there for service principals though
    def workspace_get_access_details_user(self, userName:str, workspaceName:str=None) -> list:
        if workspaceName is None:
            workspaceAccessDetails = []
            workspaceList = self.workspace_list()
            # Getting hit with throttling with so many API calls
            for workspace in workspaceList:
                accessDetailsList = [access for access in self._workspace_get_access_details_workspace_id(workspaceId=workspace.get('id')) if access.get('principal').get('displayName') == userName]
                if len(accessDetailsList) > 0:
                    workspaceAccessDetails += [{'workspaceName': workspace.get('displayName'), 'workspaceAccessDetails': accessDetailsList}]
        else:
            workspaceAccessDetails = [{'workspaceName': workspaceName, 'workspaceAccessDetails': [access for access in self.workspace_get_access_details(workspaceName=workspaceName) if access.get('principal').get('displayName') == userName][0]}]
        return workspaceAccessDetails
    

    # # TODO
    # def user_get_id(self) -> str:
    #     response = self.request(method='get', url='https://api.fabric.microsoft.com/v1/admin/users')
    #     userId = [user.get('id') for user in response.json().get('value') if user.get('displayName') == userName][0]
    #     return userId
    

    # # TODO - Need logic to get id from AAD/Entra
    # def user_get_access_entities(self, userName:str) -> requests.Response:
    #     # userId = self.users_get_id(userName=userName)
    #     response = self.request(method='get', url=f'https://api.fabric.microsoft.com/v1/admin/users/{userName}/access')
    #     return response


    def pipeline_list_response(self, workspaceName:str) -> str:
        pipelineResponse = self.item_get_response(workspaceName=workspaceName, itemType='DataPipeline')
        return pipelineResponse
    

    def pipeline_list(self, workspaceName:str) -> list:
        pipelineList = self.pipeline_list_response(workspaceName=workspaceName).json().get('value')
        return pipelineList
    

    def pipeline_create(self, workspaceName:str, pipelineName:str, pipelinePartsList:str) -> requests.Response:
        body = {"displayName": pipelineName
                ,"type": "DataPipeline"
                ,"definition": {
                    "parts": pipelinePartsList
                    }
                }
        workspaceId = self.workspace_get_id(workspaceName=workspaceName)
        response = self.request(method='post', url=f'https://api.fabric.microsoft.com/v1/workspaces/{workspaceId}/items', body=body)
        return response
    

    def pipeline_update_metadata(self, workspaceName:str, pipelineName:str, displayName:str='', description:str='') -> str:
        body = {k:v for k,v in {'displayName':displayName, 'description': description}.items() if v != ''}
        response = self.item_update_metadata(workspaceName=workspaceName, itemName=pipelineName, body=body)
        return response
    

    ## TODO -WIP
    def pipeline_update_definition(self, workspaceName:str, pipelineName:str, payloadBase64:str) -> str:
        body = {"definition": { 
                    "parts": [ 
                    { 
                        "path": "pipeline-content.json", 
                        "payload": payloadBase64, 
                        "payloadType": "InlineBase64" 
                    } 
                    ] 
                } 
            }
        response = self.item_update_definition(workspaceName=workspaceName, itemName=pipelineName, definition=body)
        return response


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
        
        response = self.request(method='post', url=f'https://api.fabric.microsoft.com/v1/workspaces/{workspaceId}/items', body=body)
        return response

    # https://learn.microsoft.com/en-us/rest/api/fabric/admin/items/list-items?tabs=HTTP
    def notebook_get_id(self, workspaceName:str, notebookName:str) -> str:
        workspaceId = self.workspace_get_id(workspaceName=workspaceName)
        response = self.request(method='get', url=f'https://api.fabric.microsoft.com/v1/workspaces/{workspaceId}/items?type=Notebook')
        notebookId = [notebook.get('id') for notebook in response.json().get('value') if notebook.get('displayName') == notebookName][0]
        return notebookId


    def notebook_get_item_definition(self, workspaceName:str, notebookName:str) -> requests.Response:
        workspaceId = self.workspace_get_id(workspaceName=workspaceName)
        notebookId = self.notebook_get_id(workspaceName=workspaceName, notebookName=notebookName)
        response = self.request(method='post', url=f'https://api.fabric.microsoft.com/v1/workspaces/{workspaceId}/items/{notebookId}/getDefinition?format=ipynb')
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
        response = self.request(method='post', url=f'https://api.fabric.microsoft.com/v1/workspaces/{workspaceIdTarget}/items', body=body)
        return response
    

    def notebook_delete(self, workspaceName:str, notebookName:str) -> str:
        response = self.item_delete(workspaceName=workspaceName, itemName=notebookName)
        return response
    

    # https://learn.microsoft.com/en-us/rest/api/fabric/admin/items/list-items?tabs=HTTP
    def item_get_response(self, workspaceName:str, itemType:str='') -> requests.Response:
        workspaceId = self.workspace_get_id(workspaceName=workspaceName)
        itemTypeFilter = f'type={itemType}' if itemType else ''
        response = self.request(method='get', url=f'https://api.fabric.microsoft.com/v1/workspaces/{workspaceId}/items?{itemTypeFilter}')
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
        response = self.request(method='post', url=f'https://api.fabric.microsoft.com/v1/workspaces/{workspaceId}/items/{itemId}/getDefinition')
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
        # logger.info(f'item_create - {body=}')
        response = self.request(method='post', url=f'https://api.fabric.microsoft.com/v1/workspaces/{workspaceId}/items', body=body)
        return response
    

    # This one does not work yet!! (for lakehouse)
    def item_delete(self, workspaceName:str, itemName:str):
        # https://wabi-west-us3-a-primary-redirect.analysis.windows.net/metadata/artifacts/0106bc1e-ab38-4a85-9569-7b9100799147
        workspaceId = self.workspace_get_id(workspaceName=workspaceName)
        itemId = self.item_get_id(workspaceName=workspaceName, itemName=itemName)
        # logger.info(f'item_delete - {workspaceName=}:{workspaceId=} - {itemName=}:{itemId}')
        url = f'https://api.fabric.microsoft.com/v1/workspaces/{workspaceId}/items/{itemId}'
        response = self.request(method='delete', url=f'https://api.fabric.microsoft.com/v1/workspaces/{workspaceId}/items/{itemId}')
        return response


    def item_update_metadata(self, workspaceName:str, itemName:str, body:dict) -> str:
        workspaceId = self.workspace_get_id(workspaceName=workspaceName)
        itemId = self.item_get_id(workspaceName=workspaceName, itemName=itemName)
        response = self.request(method='patch', url=f'https://api.fabric.microsoft.com/v1/workspaces/{workspaceId}/items/{itemId}', body=body)
        return response
    

    ## TODO WIP
    def item_update_definition(self, workspaceName:str, itemName:str, definition:dict) -> str:
        workspaceId = self.workspace_get_id(workspaceName=workspaceName)
        itemId = self.item_get_id(workspaceName=workspaceName, itemName=itemName)
        response = self.request(method='post', url=f'https://api.fabric.microsoft.com/v1/workspaces/{workspaceId}/items/{itemId}/updateDefinition', body=definition)
        return response


    def lakehouse_get_shortcut(self, workspaceName:str, itemName:str, shortcutPath:str, shortcutName:str) -> requests.Response:
        workspaceId = self.workspace_get_id(workspaceName=workspaceName)
        itemId = self.artifact_get_id(workspaceName=workspaceName, artifactName=itemName)
        # https://learn.microsoft.com/en-us/rest/api/fabric/core/onelake-shortcuts/get-shortcut?tabs=HTTP
        response = self.request(method='get', url=f'https://api.fabric.microsoft.com/v1/workspaces/{workspaceId}/items/{itemId}/shortcuts/{shortcutPath}/{shortcutName}')
        return response
    

    def _lakehouse_create_shortcut(self, workspaceId:str, itemId:str, body:dict) -> requests.Response:
        response = self.request(method='post', url=f'https://api.fabric.microsoft.com/v1/workspaces/{workspaceId}/items/{itemId}/shortcuts', body=body)
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
        response = self.request(method='get', url='https://api.powerbi.com/v2.0/myorg/me/gatewayClusterDatasources') #?$expand=users
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





