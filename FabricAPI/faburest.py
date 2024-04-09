import requests, json, logging, time, datetime, math
from typing import Literal

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

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
        # Will pause 5 unique times before failing
        for _ in range(5):
            responseStatus = self.request(method='get', url=responseLocation)
            if responseStatus.json().get('status') != 'Succeeded':
                logger.info(f'Operation {response.headers.get("x-ms-operation-id")} is not ready. Waiting for {response.headers.get("Retry-After")} seconds.')
                time.sleep(int(response.headers.get('Retry-After')))
            else:
                logger.info('Payload is ready. Requesting the result.')
                responseResult = self.request(method='get', url=f'{responseLocation}/result')
                return responseResult


    ## PBI API
    # https://learn.microsoft.com/en-us/rest/api/power-bi/capacities/get-capacities
    def capacity_list_response(self) -> requests.Response:
        # response = self.request(method='get', url='https://wabi-west-us3-a-primary-redirect.analysis.windows.net/capacities/listbyrollouts')
        response = self.request(method='get', url='https://api.powerbi.com/v1.0/myorg/capacities')
        return response
    

    def capacity_list(self) -> list:
        # capacityList = self.capacity_list_response().json().get('capacitiesMetadata')
        capacityList = self.capacity_list_response().json().get('value')
        return capacityList
    

    def capacity_get(self, capacityName:str) -> dict:
        # capacity = [capacity for capacity in self.capacity_list() if capacity.get('configuration').get('displayName') == capacityName][0]
        capacity = [capacity for capacity in self.capacity_list() if capacity.get('displayName') == capacityName][0]
        return capacity
    

    ## Unoffice API to list users
    # Should change to an entra API in the future   
    def principal_list_response(self, prefix:str) -> requests.Response:
        # response = self.request(method='get', url='https://wabi-west-us3-a-primary-redirect.analysis.windows.net/metadata/people?prefix=bret&type=3&limit=10&includeB2BUsers=true&relevantUsersFirst=true&includeRelevantGroups=false')
        response = self.request(method='get', url=f'https://wabi-west-us3-a-primary-redirect.analysis.windows.net/metadata/people?prefix={prefix}')
        return response
    

    def principal_list(self, prefix:str) -> requests.Response:
        response = self.principal_list_response(prefix).json()
        return response
    

    def principal_get_id(self, principalName:str) -> str:
        principalId = [principal.get('objectId') for principal in self.principal_list(prefix=principalName) if principal.get('userPrincipalName') == principalName][0]
        return principalId
    

    def workspace_list_response(self) -> requests.Response:
        logger.info('workspace_list_response')
        response = self.request(method='get', url='https://api.fabric.microsoft.com/v1/workspaces')
        return response
    

    def workspace_list(self) -> list:
        logger.info('workspace_list')
        workspaceList = self.workspace_list_response().json().get('value')
        return workspaceList


    def workspace_create_response(self, workspaceName:str, capacityName:str, description:str=None) -> requests.Response:
        capacityId = self.capacity_get(capacityName=capacityName).get('id')
        body = {
            "displayName": workspaceName
            ,"capacityId": capacityId
            ,**({"description": description} if description is not None else {})
        }
        response = self.request(method='post', url='https://api.fabric.microsoft.com/v1/workspaces', body=body)
        return response


    def workspace_create(self, workspaceName:str, capacityName:str, description:str=None) -> dict:
        response = self.workspace_create_response(workspaceName=workspaceName, capacityName=capacityName, description=description)
        return response.json()
    

    def workspace_delete(self, workspaceName:str) -> requests.Response:
        workspaceId = self.workspace_get_id(workspaceName=workspaceName)
        response = self.request(method='delete', url=f'https://api.fabric.microsoft.com/v1/workspaces/{workspaceId}')
        return response
    

    def workspace_assign_capacity(self, workspaceName:str, capacityName:str) -> requests.Response:
        workspaceId = self.workspace_get_id(workspaceName=workspaceName)
        capacityId = self.capacity_get(capacityName=capacityName).get('id')
        body = {"capacityId": capacityId}
        response = self.request(method='post', url=f'https://api.fabric.microsoft.com/v1/workspaces/{workspaceId}/assignToCapacity', body=body)
        return response
    

    # https://learn.microsoft.com/en-us/rest/api/fabric/core/workspaces/add-workspace-role-assignment?tabs=HTTP
    def workspace_add_role_assignment(self, workspaceName:str, principalName:str, role:Literal['Admin', 'Contributor', 'Member', 'Viewer']) -> requests.Response:
        workspaceId = self.workspace_get_id(workspaceName=workspaceName)
        principal = {'id': self.principal_get_id(principalName=principalName), "type": "User"}
        body = {'principal': principal, 'role': role}
        print(body)
        response = self.request(method='post', url=f'https://api.fabric.microsoft.com/v1/workspaces/{workspaceId}/roleAssignments', body=body)
        return response
    
    
    def workspace_delete_role_assignment(self, workspaceName:str, principalName:str) -> requests.Response:
        workspaceId = self.workspace_get_id(workspaceName=workspaceName)
        principalId = {'id': self.principal_get_id(principalName=principalName), "type": "User"}
        response = self.request(method='delete', url=f'https://api.fabric.microsoft.com/v1/workspaces/{workspaceId}/roleAssignments/{principalId}')
        return response
    

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
    

    def pipeline_create(self, workspaceName:str, pipelineName:str, pipelineDefinition:dict) -> requests.Response:
        response = self.item_create(workspaceName=workspaceName, itemName=pipelineName, itemType='DataPipeline', itemDefinition=pipelineDefinition)
        return response
    
    
    def pipeline_delete(self, workspaceName:str, pipelineName:str) -> requests.Response:
        response = self.item_delete(workspaceName=workspaceName, itemName=pipelineName)
        return response
    

    def pipeline_update_metadata(self, workspaceName:str, pipelineName:str, pipelineNameNew:str=None, pipelineDescription:str=None) -> str:
        body = {k:v for k,v in {'displayName':pipelineNameNew, 'description': pipelineDescription}.items() if v != ''}
        response = self.item_update_metadata(workspaceName=workspaceName, itemName=pipelineName, body=body)
        return response
    

    def pipeline_update_definition(self, workspaceName:str, pipelineName:str, pipelineDefinition:dict) -> str:
        # https://learn.microsoft.com/en-us/fabric/data-factory/pipeline-rest-api#update-item-definition
        response = self.item_update_definition(workspaceName=workspaceName, itemName=pipelineName, definition=pipelineDefinition)
        return response


    def pipeline_run(self, workspaceName:str, pipelineName:str) -> requests.Response:
        response = self.item_run_job(workspaceName=workspaceName, itemName=pipelineName, jobType='Pipeline')
        return response
    

    def pipeline_get_run_instance(self, workspaceName:str, pipelineName:str, jobInstanceId:str) -> requests.Response:
        response = self.item_get_job_instance(workspaceName=workspaceName, itemName=pipelineName, jobInstanceId=jobInstanceId)
        return response


    def pipeline_cancel_run_instance(self, workspaceName:str, pipelineName:str, jobInstanceId:str) -> requests.Response:
        response = self.item_cancel_job_instance(workspaceName=workspaceName, itemName=pipelineName, jobInstanceId=jobInstanceId)
        return response
    

    def pipeline_get_object(self, workspaceName:str, pipelineName:str) -> str:
        lakehouseId = self.item_get_object(workspaceName=workspaceName, itemName=pipelineName, itemType='DataPipeline')
        return lakehouseId
    

    # https://learn.microsoft.com/en-us/rest/api/fabric/admin/items/list-items?tabs=HTTP
    def pipeline_get_id(self, workspaceName:str, pipelineName:str) -> str:
        pipelineId = self.item_get_id(workspaceName=workspaceName, itemName=pipelineName, itemType='DataPipeline')
        return pipelineId


    # https://learn.microsoft.com/en-us/rest/api/fabric/core/items/get-item-definition?tabs=HTTP
    def pipeline_get_definition(self, workspaceName:str, pipelineName:str) -> list:
        itemDefinition = self.item_get_definition(workspaceName=workspaceName, itemName=pipelineName, itemType='DataPipeline')
        return itemDefinition
    

    def pipeline_get_definition_parts(self, workspaceName:str, pipelineName:str) -> list:
        itemDefinitionParts = self.item_get_definition_parts(workspaceName=workspaceName, itemName=pipelineName, itemType='DataPipeline')
        return itemDefinitionParts


    # https://learn.microsoft.com/en-us/rest/api/fabric/core/items/create-item?tabs=HTTP
    def pipeline_clone(self, workspaceNameSource:str, pipelineNameSource:str, workspaceNameTarget:str, pipelineNameTarget:str) -> requests.Response:
        pipelineDefinition = self.pipeline_get_definition(workspaceName=workspaceNameSource, pipelineName=pipelineNameSource)
        response = self.item_create(workspaceName=workspaceNameTarget, itemName=pipelineNameTarget, itemType='DataPipeline', itemDefinition=pipelineDefinition)
        return response


    # https://learn.microsoft.com/en-us/rest/api/fabric/admin/items/list-items?tabs=HTTP
    def notebook_get_id(self, workspaceName:str, notebookName:str) -> str:
        notebookId = self.item_get_id(workspaceName=workspaceName, itemName=notebookName, itemType='Notebook')
        return notebookId


    def notebook_get_item_definition(self, workspaceName:str, notebookName:str) -> requests.Response:
        definition = self.item_get_definition(workspaceName=workspaceName, itemName=notebookName, itemType='Notebook', format='ipynb')
        return definition


    def notebook_clone(self, workspaceNameSource:str, notebookNameSource:str, workspaceNameTarget:str, notebookNameTarget:str) -> requests.Response:
        notebook_get_definition = self.notebook_get_item_definition(workspaceName=workspaceNameSource, notebookName=notebookNameSource)
        response = self.item_create(workspaceName=workspaceNameTarget, itemName=notebookNameTarget, itemType='Notebook', itemDefinition=notebook_get_definition)
        return response
    

    def notebook_delete(self, workspaceName:str, notebookName:str) -> str:
        response = self.item_delete(workspaceName=workspaceName, itemName=notebookName)
        return response
    

    # https://learn.microsoft.com/en-us/rest/api/fabric/admin/items/list-items?tabs=HTTP
    def item_get_response(self, workspaceName:str, itemType:str=None) -> requests.Response:
        workspaceId = self.workspace_get_id(workspaceName=workspaceName)
        # itemTypeFilter = f'type={itemType}' if itemType else ''
        # response = self.request(method='get', url=f'https://api.fabric.microsoft.com/v1/workspaces/{workspaceId}/items?{itemTypeFilter}')
        response = self.request(method='get', url=f'https://api.fabric.microsoft.com/v1/workspaces/{workspaceId}/items{f'?type={itemType}' if itemType else ''}')
        return response


    def item_list(self, workspaceName:str, itemType:str=None) -> list:
        item_get_response = self.item_get_response(workspaceName=workspaceName, itemType=itemType)
        item_list = item_get_response.json().get('value')
        return item_list


    def item_get_object(self, workspaceName:str, itemName:str, itemType:str=None) -> dict:
        item_list = self.item_list(workspaceName=workspaceName, itemType=itemType)
        artifact = [item for item in item_list if item.get('displayName') == itemName][0]
        return artifact


    def item_get_id(self, workspaceName:str, itemName:str, itemType:str=None) -> str:
        artifactObject = self.item_get_object(workspaceName=workspaceName, itemName=itemName, itemType=itemType)
        artifactId = artifactObject.get('id')
        return artifactId


    def item_get_definition_response(self, workspaceName:str, itemName:str, itemType:str=None, format=None) -> requests.Response:
        workspaceId = self.workspace_get_id(workspaceName=workspaceName)
        itemId = self.item_get_id(workspaceName=workspaceName, itemName=itemName, itemType=itemType)
        # logger.info(f'item_get_definition_response {workspaceName=}:{workspaceId=} - {itemName=}:{itemId}')
        response = self.request(method='post', url=f'https://api.fabric.microsoft.com/v1/workspaces/{workspaceId}/items/{itemId}/getDefinition{f'?format={format}' if format else ''}')
        return response
    

    def item_get_definition(self, workspaceName:str, itemName:str, itemType:str='', format=None) -> dict:
        response = self.item_get_definition_response(workspaceName=workspaceName, itemName=itemName, itemType=itemType).json()
        return response
    

    def item_get_definition_parts(self, workspaceName:str, itemName:str, itemType:str='', format=None) -> list:
        response = self.item_get_definition(workspaceName=workspaceName, itemName=itemName, itemType=itemType, format=format).get('definition').get('parts')
        return response


    def item_create(self, workspaceName:str, itemName:str, itemType:str, itemDefinition:dict=None) -> requests.Response:
        workspaceId = self.workspace_get_id(workspaceName=workspaceName)
        body = {"displayName": itemName
                ,"type": itemType
                ,**(itemDefinition if itemDefinition is not None else {})
                # ,**({'definition': itemDefinition.get('definition')} if itemDefinition is not None else {}) ## This can be None when creating a Lakehouse
                }
        response = self.request(method='post', url=f'https://api.fabric.microsoft.com/v1/workspaces/{workspaceId}/items', body=body)
        return response
    

    # This one does not work yet!! (for lakehouse)
    def item_delete(self, workspaceName:str, itemName:str):
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


    #
    #   jobType = ['Pipeline']
    #
    def item_run_job(self, workspaceName:str, itemName:str, jobType:Literal['Pipeline']) -> requests.Response:
        workspaceId = self.workspace_get_id(workspaceName=workspaceName)
        itemId = self.item_get_id(workspaceName=workspaceName, itemName=itemName)
        # response = self.request(method='post', url=f'https://api.fabric.microsoft.com/v1/workspaces/{workspaceId}/items/{itemId}/jobs/instances{f'?jobType={jobType}' if jobType else ''}')
        # print(f'{response=}')
        # print(f'{response.headers=}')
        ## exception because in preview, the job instance id is found in the response header and not body so we have to use requests directly.
        logger.info('item_run_job')
        response = requests.request(method='post', url=f'https://api.fabric.microsoft.com/v1/workspaces/{workspaceId}/items/{itemId}/jobs/instances{f'?jobType={jobType}' if jobType else ''}', headers=self.header)
        return response.headers.get('Location').split('jobs/instances/')[-1]
    

    def item_get_job_instance_response(self, workspaceName:str, itemName:str, jobInstanceId:str) -> requests.Response:
        workspaceId = self.workspace_get_id(workspaceName=workspaceName)
        itemId = self.item_get_id(workspaceName=workspaceName, itemName=itemName)
        response = self.request(method='get', url=f'https://api.fabric.microsoft.com/v1/workspaces/{workspaceId}/items/{itemId}/jobs/instances/{jobInstanceId}')
        return response
    
    
    def item_get_job_instance(self, workspaceName:str, itemName:str, jobInstanceId:str) -> dict:
        response = self.item_get_job_instance_response(workspaceName=workspaceName, itemName=itemName, jobInstanceId=jobInstanceId).json()
        return response
    
    
    def item_cancel_job_instance(self, workspaceName:str, itemName:str, jobInstanceId:str) -> requests.Response:
        workspaceId = self.workspace_get_id(workspaceName=workspaceName)
        itemId = self.item_get_id(workspaceName=workspaceName, itemName=itemName)
        response = self.request(method='post', url=f'https://api.fabric.microsoft.com/v1/workspaces/{workspaceId}/items/{itemId}/jobs/instances/{jobInstanceId}/cancel')
        return response


    def lakehouse_get_shortcut(self, workspaceName:str, itemName:str, shortcutPath:str, shortcutName:str) -> requests.Response:
        workspaceId = self.workspace_get_id(workspaceName=workspaceName)
        itemId = self.item_get_id(workspaceName=workspaceName, artifactName=itemName)
        # https://learn.microsoft.com/en-us/rest/api/fabric/core/onelake-shortcuts/get-shortcut?tabs=HTTP
        response = self.request(method='get', url=f'https://api.fabric.microsoft.com/v1/workspaces/{workspaceId}/items/{itemId}/shortcuts/{shortcutPath}/{shortcutName}')
        return response
    

    def _lakehouse_create_shortcut(self, workspaceId:str, itemId:str, body:dict) -> requests.Response:
        response = self.request(method='post', url=f'https://api.fabric.microsoft.com/v1/workspaces/{workspaceId}/items/{itemId}/shortcuts', body=body)
        return response


    # TODO
    def lakehouse_create_shortcut_adls(self, workspaceName:str, itemName:str, shortcutName:str, shortcutPath:str, adlsPath:str, adlsSubPath:str) -> requests.Response:
        workspaceId = self.workspace_get_id(workspaceName=workspaceName)
        itemId = self.item_get_id(workspaceName=workspaceName, artifactName=itemName)
        connectionId = '' # TODO
        # https://learn.microsoft.com/en-us/rest/api/fabric/core/onelake-shortcuts/create-shortcut?tabs=HTTP#adlsgen2
        # https://learn.microsoft.com/en-us/rest/api/fabric/core/onelake-shortcuts/create-shortcut?tabs=HTTP#target
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


    def connection_response(self, connectionName:str) -> requests.Response:
        response = self.request(method='get', url='https://api.powerbi.com/v2.0/myorg/me/gatewayClusterDatasources')
        return response


    def connection_list(self, connectionName:str) -> list:
        connection_list = self.connection_response(connectionName=connectionName).json().get('value')
        return connection_list
    

    def connection_get_object(self, connectionName:str) -> dict:
        connectionId = [connection for connection in self.connection_list(connectionName=connectionName) if connection.get('datasourceName') == connectionName][0]
        return connectionId
    
    
    def connection_get_id(self, connectionName:str) -> str:
        connectionId = self.connection_get_object(connectionName=connectionName).get('id')
        return connectionId
    

    # https://learn.microsoft.com/en-us/rest/api/power-bi/gateways/create-datasource
    # def connection_create(self, connectionName:str, connectionDefinition:dict) -> requests.Response:
    #     pass
    

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

    
    # https://learn.microsoft.com/en-us/fabric/data-engineering/lakehouse-api#get-lakehouse-properties
    def lakehouse_get_properties_response(self, workspaceName:str, lakehouseName:str) -> dict:
        workspaceId = self.workspace_get_id(workspaceName=workspaceName)
        lakehouseId = self.lakehouse_get_id(workspaceName=workspaceName, lakehouseName=lakehouseName)
        lakehouseProperties = self.request(method='get', url=f'https://api.fabric.microsoft.com/v1/workspaces/{workspaceId}/lakehouses/{lakehouseId}')
        return lakehouseProperties


    def lakehouse_get_properties(self, workspaceName:str, lakehouseName:str) -> dict:
        lakehouseProperties = self.lakehouse_get_properties_response(workspaceName=workspaceName, lakehouseName=lakehouseName).json()
        return lakehouseProperties
    

    ## Is not supported yet
    # https://learn.microsoft.com/en-us/fabric/data-engineering/lakehouse-api#update-a-lakehouse
    def lakehouse_update(self, workspaceName:str, lakehouseName:str, lakehouseNameNew:str=None, lakehouseDescription:str=None) -> str:
        body = {**({"displayName": lakehouseNameNew} if lakehouseNameNew is not None else {})
                ,**({"description": lakehouseDescription} if lakehouseDescription is not None else {})
            }
        response = self.item_update_metadata(workspaceName=workspaceName, itemName=lakehouseName, body=body)
        return response
    

    def lakehouse_get_tables_response(self, workspaceName:str, lakehouseName:str) -> requests.Response:
        workspaceId = self.workspace_get_id(workspaceName=workspaceName)
        lakehouseId = self.lakehouse_get_id(workspaceName=workspaceName, lakehouseName=lakehouseName)
        response = self.request(method='get', url=f'https://api.fabric.microsoft.com/v1/workspaces/{workspaceId}/lakehouses/{lakehouseId}/tables')
        return response


    def lakehouse_get_tables(self, workspaceName:str, lakehouseName:str) -> list:
        lakehouseTableList = self.lakehouse_get_tables_response(workspaceName=workspaceName, lakehouseName=lakehouseName).json().get('data')
        return lakehouseTableList




