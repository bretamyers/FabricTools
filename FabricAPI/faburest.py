import requests, json, logging, time, datetime, math, base64
from typing import Literal, List
import auth

logger = logging.getLogger(__name__)
# logger.setLevel(logging.INFO)

class fabric_rest():
    def __init__(self, audience:str='pbi'):
        self.header = self.create_header(audience)
        # credential = auth.Interactive()
        # self.header_fabric = credential.create_header(credential.get_token_fabric())


    def create_header(self, audience:str='pbi') -> dict:
        import _util
        token = _util._get_token_cached()
        return {'Authorization': f'Bearer {token}', 'Content-type': 'application/json'}


    def _base64_decode_bytes(self, base64String:str) -> bytes:
        return base64.b64decode(base64String.encode('utf-8'))


    def base64_decode(self, base64String:str) -> str:
        return self._base64_decode_bytes(base64String).decode('utf-8')
    

    # def request(self, method:str, url:str, body:dict=None) -> requests.Response:
    #     try:
    #         response = requests.request(method=method, url=url, headers=self.header, data=json.dumps(body))
    #         logger.debug(response.json())
    #         response.raise_for_status()
    #         logger.debug(f"Response - {response.status_code}")
    #         if response.status_code == 202:
    #             response = self.response_long_running(response=response)
    #         return response
    #     except requests.exceptions.HTTPError as errh:
    #         ## Add a step to check if the error is due to throttling and wait until the restriction is lifted
    #         if 'Request is blocked by the upstream service until:' in errh.response.json()['message']:
    #             blockedDatetime = datetime.datetime.strptime(errh.response.json()['message'].split('Request is blocked by the upstream service until: ')[1], '%m/%d/%Y %I:%M:%S %p')
    #             sleepDuration = math.ceil((blockedDatetime - datetime.datetime.now(datetime.UTC).replace(tzinfo=None)).total_seconds())
    #             logger.info(f"Sleeping for {sleepDuration} seconds")
    #             time.sleep(sleepDuration) # pause until we can make the request again
    #             return self.request(method=method, url=url, body=body)
    #         raise Exception("Http Error:", errh.response.text)
    #     except requests.exceptions.ConnectionError as errc:
    #         raise Exception("Error Connecting:", errc.response.text)
    #     except requests.exceptions.Timeout as errt:
    #         raise Exception("Timeout Error:", errt.response.text)
    #     except requests.exceptions.RequestException as err:
    #         raise Exception(response.status_code)
    #         # print("Error", err.response.text)


    # def request(self, method:str, url:str, body:dict=None, responseList:list=[]) -> List[requests.Response]:
    #     try:
    #         response = requests.request(method=method, url=url, headers=self.header, data=json.dumps(body))
    #         logger.debug(response.json())
    #         response.raise_for_status()
    #         logger.debug(f"Response - {response.status_code}")
    #         if response.status_code == 202:
    #             response = self.response_long_running(response=response)
            
    #         responseList.append(response)
    #         if response.json().get('continuationUri') is not None and response.json().get('continuationToken') is not None:
    #             response = self.request(method='get', url=f'{response.json().get("continuationUri")}', responseList=responseList)
    #         return responseList
    #     except requests.exceptions.HTTPError as errh:
    #         ## Add a step to check if the error is due to throttling and wait until the restriction is lifted
    #         if 'Request is blocked by the upstream service until:' in errh.response.json()['message']:
    #             blockedDatetime = datetime.datetime.strptime(errh.response.json()['message'].split('Request is blocked by the upstream service until: ')[1], '%m/%d/%Y %I:%M:%S %p')
    #             sleepDuration = math.ceil((blockedDatetime - datetime.datetime.now(datetime.UTC).replace(tzinfo=None)).total_seconds())
    #             logger.info(f"Sleeping for {sleepDuration} seconds")
    #             time.sleep(sleepDuration) # pause until we can make the request again
    #             return self.request(method=method, url=url, body=body)
    #         raise Exception("Http Error:", errh.response.text)
    #     except requests.exceptions.ConnectionError as errc:
    #         raise Exception("Error Connecting:", errc.response.text)
    #     except requests.exceptions.Timeout as errt:
    #         raise Exception("Timeout Error:", errt.response.text)
    #     except requests.exceptions.RequestException as err:
    #         raise Exception(response.status_code)
    #         # print("Error", err.response.text)

    def request(self, method:str, url:str, body:dict=None) -> List[requests.Response]:
        logger.info(f'request - {method} - {url} - {body}')
        responseList = []
        def make_request(method:str, url:str, body:dict=None):
            try:
                # logger.info(f'request.make_request - {method} - {url} - {body}')
                response = requests.request(method=method, url=url, headers=self.header, data=json.dumps(body))
                logger.debug(response.json())
                response.raise_for_status()
                logger.debug(f"Response - {response.status_code}")
                if response.status_code == 202:
                    response = self.response_long_running(response=response)
                
                responseList.append(response)
                # logger.info(f'make_request - continuationUri: {response.json().get("continuationUri")}')
                if response.json().get('continuationUri') is not None and response.json().get('continuationToken') is not None:
                    logger.info(f'ContinuationUri - {response.json().get("continuationUri")}')
                    response = self.request(method='get', url=f'{response.json().get("continuationUri")}', responseList=responseList)
            except requests.exceptions.HTTPError as errh:
                ## Add a step to check if the error is due to throttling and wait until the restriction is lifted
                if 'Request is blocked by the upstream service until:' in errh.response.json()['message']:
                    blockedDatetime = datetime.datetime.strptime(errh.response.json()['message'].split('Request is blocked by the upstream service until: ')[1], '%m/%d/%Y %I:%M:%S %p')
                    sleepDuration = math.ceil((blockedDatetime - datetime.datetime.now(datetime.UTC).replace(tzinfo=None)).total_seconds())
                    logger.info(f"Sleeping for {sleepDuration} seconds")
                    time.sleep(sleepDuration) # pause until we can make the request again
                    #return self.request(method=method, url=url, body=body) # need to look into this. This might be the casuing the error
                    make_request(method=method, url=url, body=body)
                raise Exception("Http Error:", errh.response.text)
            except requests.exceptions.ConnectionError as errc:
                raise Exception("Error Connecting:", errc.response.headers)
            except requests.exceptions.Timeout as errt:
                raise Exception("Timeout Error:", errt.response.text)
            except requests.exceptions.RequestException as err:
                raise Exception(response.status_code)
                
        make_request(method=method, url=url, body=body)
        logger.info(f'responseList - {responseList}')
        return responseList


    def response_long_running(self, response:requests.Response) -> requests.Response:
        responseLocation = response.headers.get('Location')
        # Will pause 5 unique times before failing
        for _ in range(5):
            # locationResponse = self.request(method='get', url=responseLocation)
            # logger.info(f'{locationResponse}, {len(locationResponse)}')
            responseStatus = self.request(method='get', url=responseLocation)[0] # Just get the first item in the list because it should only have one item.
            if responseStatus.json().get('status') != 'Succeeded':
                logger.info(f'Operation {response.headers.get("x-ms-operation-id")} is not ready. Waiting for {response.headers.get("Retry-After")} seconds.')
                time.sleep(int(response.headers.get('Retry-After')))
            else:
                if responseStatus.headers.get('Location') is None:
                    logger.info('Long running operation has completed. No result to be returned')
                    return responseStatus
                else:
                    logger.info('Payload is ready. Requesting the result.')
                    # responseResult = self.request(method='get', url=f'{responseLocation}/result')
                    responseResult = self.request(method='get', url=responseStatus.headers.get('Location'))[0] # Just get the first item in the list because it should only have one item.
                    return responseResult
                

    def response_parse(self, response:List[requests.Response]) -> dict:
        responseParsed = [response.json() for response in response]
        return responseParsed
    

    def response_list_unravel(self, responseList:List[requests.Response], param:str='value') -> list:
        responseUnraveled = []
        for responseItem in self.response_parse(responseList):
            if responseItem.get(param) is not None:
                for response in responseItem.get(param):
                    responseUnraveled.append(response)
            else:
                responseUnraveled.append(responseItem)
        return responseUnraveled
    

    def response_build_parameters(self, **paramaters:dict) -> str:
        parameterString = '&'.join([f'{k}={v}' for k,v in paramaters.items() if v is not None])
        responseParameterString = f'?{parameterString}' if parameterString != '' else ''
        return responseParameterString


    # https://learn.microsoft.com/en-us/rest/api/power-bi/capacities/get-capacities
    def capacity_list_response(self) -> requests.Response:
        response = self.request(method='get', url=f'https://api.fabric.microsoft.com/v1/capacities')
        return response
    

    def capacity_list(self) -> list:
        capacityList = self.response_list_unravel(responseList=self.capacity_list_response(), param='value')
        return capacityList
    

    def capacity_get(self, capacityName:str) -> dict:
        capacity = [capacity for capacity in self.capacity_list() if capacity.get('displayName') == capacityName][0]
        return capacity
    

    ## Unoffical API to list users
    # Should change to an entra API in the future   
    def principal_list_response(self, prefix:str) -> requests.Response:
        # response = self.request(method='get', url='https://wabi-west-us3-a-primary-redirect.analysis.windows.net/metadata/people?prefix=bret&type=3&limit=10&includeB2BUsers=true&relevantUsersFirst=true&includeRelevantGroups=false')
        response = self.request(method='get', url=f'https://wabi-west-us3-a-primary-redirect.analysis.windows.net/metadata/people?prefix={prefix}')
        return response
    

    def principal_list(self, prefix:str='') -> requests.Response:
        response = self.principal_list_response(prefix)
        return response
    

    def principal_get_id(self, principalName:str) -> str:
        principalId = [principal.get('objectId') for principal in self.principal_list(prefix=principalName) if principal.get('userPrincipalName') == principalName][0]
        return principalId
    

    def principal_list_access_response(self, principalName:str) -> list:
        response = self.request(method='get', url=f'https://api.fabric.microsoft.com/v1/admin/users/{principalName}/access')
        return response
    

    def principal_list_access(self, principalName:str) -> list:
        principalAccess = [response.json() for response in self.principal_list_access_response(principalName=principalName)]
        return principalAccess


    def workspace_list_response(self, capacity:str=None, name:str=None, state:str=None, type:str=None) -> List[requests.Response]:
        logger.info('workspace_list_response')
        url = f'https://api.fabric.microsoft.com/v1/workspaces{self.response_build_parameters(capacity=capacity, name=name, state=state, type=type)}'
        response = self.request(method='get', url=url)
        return response
    

    def workspace_list(self, capacity:str=None, name:str=None, state:str=None, type:str=None) -> list:
        logger.info('workspace_list')
        workspaceList = self.response_list_unravel(responseList=self.workspace_list_response(capacity=capacity, name=name, state=state, type=type), param='value')
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
        # response = [response for response in self.workspace_create_response(workspaceName=workspaceName, capacityName=capacityName, description=description)]
        response = self.response_list_unravel(self.workspace_create_response(workspaceName=workspaceName, capacityName=capacityName, description=description), param='value')
        return response
    

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
        response = self.request(method='post', url=f'https://api.fabric.microsoft.com/v1/workspaces/{workspaceId}/roleAssignments', body=body)
        return response
    
    
    def workspace_delete_role_assignment(self, workspaceName:str, principalName:str) -> requests.Response:
        workspaceId = self.workspace_get_id(workspaceName=workspaceName)
        principalId = {'id': self.principal_get_id(principalName=principalName), "type": "User"}
        response = self.request(method='delete', url=f'https://api.fabric.microsoft.com/v1/workspaces/{workspaceId}/roleAssignments/{principalId}')
        return response
    

    # https://learn.microsoft.com/en-us/rest/api/fabric/admin/workspaces/list-workspaces?tabs=HTTP
    def workspace_get_id(self, workspaceName:str) -> str:
        logger.info(f'workspace_get_id: {workspaceName=}')
        workspaceId = [workspace.get('id') for workspace in self.workspace_list() if workspace.get('displayName') == workspaceName][0]
        return workspaceId
    

    def _workspace_update_metadata(self, workspaceName:str, workspaceNameTarget:str=None, workspaceDescription:str=None) -> requests.Response:
        workspaceId = self.workspace_get_id(workspaceName=workspaceName)
        body = {k:v for k,v in {'displayName':workspaceNameTarget, 'description': workspaceDescription}.items() if v != ''}
        response = self.request(method='patch', url=f'https://api.fabric.microsoft.com/v1/workspaces/{workspaceId}', body=body)
        return response


    def workspace_rename(self, workspaceName:str, workspaceNameTarget:str) -> requests.Response:
        response = self._workspace_update_metadata(workspaceName=workspaceName, workspaceNameTarget=workspaceNameTarget)
        return response
    

    def workspace_set_description(self, workspaceName:str, workspaceDescription:str) -> requests.Response:
        response = self._workspace_update_metadata(workspaceName=workspaceName, workspaceDescription=workspaceDescription)
        return response
    

    def workspace_get_access_details_response(self, workspaceName:str) -> requests.Response:
        workspaceId = self.workspace_get_id(workspaceName=workspaceName)
        response = self.request(method='get', url=f'https://api.fabric.microsoft.com/v1/admin/workspaces/{workspaceId}/users')
        return response
    

    def _workspace_get_access_details_response_workspace_id(self, workspaceId:str) -> requests.Response:
        response = self.request(method='get', url=f'https://api.fabric.microsoft.com/v1/admin/workspaces/{workspaceId}/users')
        return response


    def workspace_get_access_details(self, workspaceName:str) -> list:
        workspaceAccessDetail = self.response_list_unravel(self.workspace_get_access_details_response(workspaceName=workspaceName), param='accessDetails')
        return workspaceAccessDetail
    

    def _workspace_get_access_details_workspace_id(self, workspaceId:str) -> list:
        workspaceAccessDetail = self.response_list_unravel(self._workspace_get_access_details_response_workspace_id(workspaceId=workspaceId), param='accessDetails')
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
        pipelineList = self.response_list_unravel(self.pipeline_list_response(workspaceName=workspaceName), param='value')
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
    def item_get_response(self, workspaceName:str, itemType:Literal['Dashboard', 'DataPipeline', 'Datamart', 'Eventstream', 'KQLDataConnection', 'KQLDatabase', 'KQLQueryset', 'Lakehouse', 'MLExperiment', 'MLModel', 'MirroredWarehouse', 'Notebook', 'PaginatedReport', 'Report', 'SQLEndpoint', 'SemanticModel', 'SparkJobDefinition', 'Warehouse']=None) -> List[requests.Response]:
        workspaceId = self.workspace_get_id(workspaceName=workspaceName)
        # itemTypeFilter = f'type={itemType}' if itemType else ''
        # response = self.request(method='get', url=f'https://api.fabric.microsoft.com/v1/workspaces/{workspaceId}/items?{itemTypeFilter}')
        response = self.request(method='get', url=f'https://api.fabric.microsoft.com/v1/workspaces/{workspaceId}/items{f'?type={itemType}' if itemType else ''}')
        return response


    def item_list(self, workspaceName:str, itemType:Literal['Dashboard', 'DataPipeline', 'Datamart', 'Eventstream', 'KQLDataConnection', 'KQLDatabase', 'KQLQueryset', 'Lakehouse', 'MLExperiment', 'MLModel', 'MirroredWarehouse', 'Notebook', 'PaginatedReport', 'Report', 'SQLEndpoint', 'SemanticModel', 'SparkJobDefinition', 'Warehouse']=None) -> list:
        item_get_response = self.item_get_response(workspaceName=workspaceName, itemType=itemType)
        item_list = self.response_list_unravel(item_get_response, param='value')
        return item_list


    def item_get_object(self, workspaceName:str, itemName:str, itemType:str=None) -> dict:
        item_list = self.item_list(workspaceName=workspaceName, itemType=itemType)
        try:
            artifact = [item for item in item_list if item.get('displayName') == itemName][0]
            return artifact
        except IndexError as ie:
            print(f'Item {itemName} not found in workspace {workspaceName} - {ie}')


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
        response = self.response_list_unravel(self.item_get_definition_response(workspaceName=workspaceName, itemName=itemName, itemType=itemType), param=None)[0]
        return response
    

    def item_get_definition_parts(self, workspaceName:str, itemName:str, itemType:str='', format=None) -> list:
        response = self.item_get_definition(workspaceName=workspaceName, itemName=itemName, itemType=itemType, format=format).get('definition').get('parts')
        return response


    def item_create(self, workspaceName:str, itemName:str, itemType:str, itemDefinition:dict=None) -> requests.Response:
        logger.info(f'item_create - {workspaceName=}:{itemName=}:{itemType=}')
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
        logger.info(f'item_delete - {workspaceName=}:{workspaceId=} - {itemName=}:{itemId}')
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
        connection_list = self.response_list_unravel(self.connection_response(connectionName=connectionName), param='value')
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
        lakehouseTableList = self.response_list_unravel(self.lakehouse_get_tables_response(workspaceName=workspaceName, lakehouseName=lakehouseName), param='data')
        return lakehouseTableList


    def sqlendpoint_list_response(self, workspaceName:str) -> requests.Response:
        sqlendpointResponse = self.item_get_response(workspaceName=workspaceName, itemType='SqlEndpoint')
        return sqlendpointResponse


    def sqlendpoint_list(self, workspaceName:str) -> list:
        sqlendpointList = self.response_list_unravel(self.sqlendpoint_list_response(workspaceName=workspaceName), param='value')
        return sqlendpointList


    def domain_list_response(self) -> requests.Response:
        domainResponse = self.request(method='get', url=f'https://api.fabric.microsoft.com/v1/admin/domains')
        return domainResponse
    
    
    def domain_list(self) -> list:
        domainList = self.response_list_unravel(self.domain_list_response(), param='domains')
        return domainList
    

    def domain_get(self, domainName:str) -> dict:
        domain = [domain for domain in self.domain_list() if domain.get('displayName') == domainName][0]
        return domain


    def domain_list_workspaces_response(self, domainName:str) -> list:
        domainId = self.domain_get(domainName=domainName).get('id')
        domainWorkspaces = self.request(method='get', url=f'https://api.fabric.microsoft.com/v1/admin/domains/{domainId}/workspaces')
        return domainWorkspaces
    

    def domain_list_workspaces(self, domainName:str) -> list:
        domainWorkspaces = self.response_list_unravel(self.domain_list_workspaces_response(domainName=domainName), param='value')
        return domainWorkspaces
    

    def domain_create_response(self, domainName:str, description:str=None, parentDomainId:str=None) -> requests.Response:
        # body = {"displayName": domainName
        #         ,**({"description": description} if description is not None else {})
        #         ,**({"parentDomainId": parentDomainId} if parentDomainId is not None else {})
        #         }
        body = {k:v for k,v in 
                    {'displayName':domainName
                     ,'description': description
                     ,'parentDomainId': parentDomainId
                    }.items() if v != ''
                }
        response = self.request(method='post', url=f'https://api.fabric.microsoft.com/v1/admin/domains', body=body)
        return response


    def domain_create(self, domainName:str, description:str=None, parentDomainId:str=None) -> dict:
        response = self.domain_create_response(domainName=domainName, description=description, parentDomainId=parentDomainId)
        return response.json()


    def domain_delete(self, domainName:str) -> requests.Response:
        domainId = self.domain_get(domainName=domainName).get('id')
        response = self.request(method='delete', url=f'https://api.fabric.microsoft.com/v1/admin/domains/{domainId}')
        return response
    

    def domain_assign_capacity(self, domainName:str, capacityName:str) -> requests.Response:
        domainId = self.domain_get(domainName=domainName).get('id')
        capacityId = self.capacity_get(capacityName=capacityName).get('id')
        body = {'capacitiesIds': [capacityId]}
        response = self.request(method='post', url=f'https://api.fabric.microsoft.com/v1/admin/domains/{domainId}/assignWorkspacesByCapacities', body=body)
        return response


    def domain_assign_workspace(self, domainName:str, workspaceName:str):
        domainId = self.domain_get(domainName=domainName).get('id')
        workspaceId = self.workspace_get_id(workspaceName=workspaceName)
        body = {'workspacesIds': [workspaceId]}
        response = self.request(method='post', url=f'https://api.fabric.microsoft.com/v1/admin/domains/{domainId}/assignWorkspaces', body=body)
        return response
    

    ## TODO - Need to check if there is a better way to get the principalId
    def domain_assign_principals(self, domainName:str, principalName:str):
        domainId = self.domain_get(domainName=domainName).get('id')
        principalId = self.workspace_get_id(workspaceName=principalName)
        body = {'principals': [principalId]}
        response = self.request(method='post', url=f'https://api.fabric.microsoft.com/v1/admin/domains/{domainId}/assignWorkspacesByPrincipals', body=body)
        return response
    

    def domain_unassign_all_workspaces(self, domainName:str):
        domainId = self.domain_get(domainName=domainName).get('id')
        response = self.request(method='post', url=f'https://api.fabric.microsoft.com/v1/admin/domains/{domainId}/unassignAllWorkspaces')
        return response
    

    def domain_unassign_workspace(self, domainName:str, workspaceName:str):
        domainId = self.domain_get(domainName=domainName).get('id')
        workspaceId = self.workspace_get_id(workspaceName=workspaceName)
        body = {'workspacesIds': [workspaceId]}
        response = self.request(method='post', url=f'https://api.fabric.microsoft.com/v1/admin/domains/{domainId}/unassignWorkspaces', body=body)
        return response


    def domain_update(self, domainName:str, domainNameChange:str=None, description:str=None, parentDomainId:str=None):
        domainId = self.domain_get(domainName=domainName).get('id')
        body = {k:v for k,v in 
                    {'displayName':domainNameChange
                     ,'description': description
                     ,'contributorsScope': parentDomainId
                    }.items() if v != ''
                }
        response = self.request(method='patch', url=f'https://api.fabric.microsoft.com/v1/admin/domains/{domainId}', body=body)
        return response
    

    def warehouse_list_response(self, workspaceName:str) -> List[requests.Response]:
        warehouseResponse = self.item_get_response(workspaceName=workspaceName, itemType='Warehouse')
        return warehouseResponse
    
    
    def warehouse_list(self, workspaceName:str) -> List:
        logger.info(f'warehouse_list: {workspaceName=}')
        warehouseResponse = self.response_list_unravel(self.warehouse_list_response(workspaceName=workspaceName), param='value')
        return warehouseResponse
    
    # ## Feature is not available yet. 6/10/2024
    # def warehouse_get_definition(self, workspaceName:str, warehouseName:str) -> requests.Response:
    #     warehouseResponse = self.item_get_definition(workspaceName=workspaceName, itemName=warehouseName, itemType='Warehouse')
    #     return warehouseResponse

    def warehouse_create_response(self, workspaceName:str, warehouseName:str) -> requests.Response:
        logger.info(f'warehouse_create_response: {workspaceName=}, {warehouseName=}')
        warehouseResponse = self.item_create(workspaceName=workspaceName, itemName=warehouseName, itemType='Warehouse')
        return warehouseResponse
    
    
    def warehouse_create(self, workspaceName:str, warehouseName:str):
        warehouseResponse = self.warehouse_create_response(workspaceName=workspaceName, warehouseName=warehouseName)[0].json()
        return warehouseResponse


    ## Does not work yet (6/10/2024). "errorCode":"BadRequest","message":"Target entity does not support the required operation"
    def warehouse_delete_response(self, workspaceName:str, warehouseName:str):
        logger.info(f'warehouse_delete_response: {workspaceName=}, {warehouseName=}')
        # warehouseResponse = self.item_delete(workspaceName=workspaceName, itemName=warehouseName)
        workspaceId = self.workspace_get_id(workspaceName=workspaceName)
        warehouseId = self.item_get_id(workspaceName=workspaceName, itemName=warehouseName)
        # response = self.request(method='delete', url=f'https://api.fabric.microsoft.com/v1/workspaces/{workspaceId}/warehouses/{warehouseId}')
        # return response
        raise NotImplementedError('Is not supported yet')
        

    # Does not work yet (6/10/2024). "Operation not supported for requested item"
    def warehouse_update_response(self, workspaceName:str, warehouseName:str, warehouseNameNew:str=None, warehouseDescription:str=None) -> requests.Response:
        workspaceId = self.workspace_get_id(workspaceName=workspaceName)
        warehouseId = self.item_get_id(workspaceName=workspaceName, itemName=warehouseName)
        body = {k:v for k,v in 
                    {'displayName':warehouseNameNew
                     ,'description': warehouseDescription
                    }.items() if v != ''
                }
        # response = self.item_update_metadata(workspaceName=workspaceName, itemName=warehouseName, body=body)
        url = f'https://api.fabric.microsoft.com/v1/workspaces/{workspaceId}/warehouses/{warehouseId}'
        logger.info(f'warehouse_create_response: {workspaceName=}, {warehouseName=}, {url=}, {body=}')
        # response = self.request(method='patch', url=f'https://api.fabric.microsoft.com/v1/workspaces/{workspaceId}/warehouses/{warehouseId}', body=body)
        # return response
        raise NotImplementedError('Is not supported yet')


    def deployment_pipelines_list_response(self) -> List[requests.Response]:
        logger.info(f'deployment_pipelines_list_response')
        response = self.request(method='get', url=f'https://api.fabric.microsoft.com/v1/deploymentPipelines')
        return response
    
    
    def deployment_pipelines_list(self) -> List[requests.Response]:
        logger.info(f'deployment_pipelines_list')
        response = self.response_list_unravel(self.deployment_pipelines_list_response(), param='value')
        return response
    

    def deployment_pipelines_get_id(self, deploymentPipelineName:str) -> str:
        deploymentPipelineId = [deploymentPipeline.get('id') for deploymentPipeline in self.deployment_pipelines_list() if deploymentPipeline.get('displayName') == deploymentPipelineName][0]
        return deploymentPipelineId
    

    def deployment_pipelines_list_stages_response(self, deploymentPipelineName:str) -> List[requests.Response]:
        logger.info(f'deployment_pipelines_list_stages_response: {deploymentPipelineName=}')
        deploymentPipelineId = self.deployment_pipelines_get_id(deploymentPipelineName=deploymentPipelineName)
        response = self.request(method='get', url=f'https://api.fabric.microsoft.com/v1/deploymentPipelines/{deploymentPipelineId}/stages')
        return response

    
    def deployment_pipelines_list_stages(self, deploymentPipelineName:str) -> list:
        logger.info(f'deployment_pipelines_list_stages: {deploymentPipelineName=}')
        response = self.response_list_unravel(self.deployment_pipelines_list_stages_response(deploymentPipelineName=deploymentPipelineName), param='value')
        return response


    def deployment_pipelines_get_stage_id(self, deploymentPipelineName:str, deploymentPipelineStageName) -> str:
        deploymentPipelineStageId = [deploymentPipelineStage.get('id') for deploymentPipelineStage in self.deployment_pipelines_list_stages(deploymentPipelineName=deploymentPipelineName) if deploymentPipelineStage.get('displayName') == deploymentPipelineStageName][0]
        return deploymentPipelineStageId
    

    def deployment_pipelines_deploy_stage_response(self, deploymentPipelineName:str, sourceStageName:str, targetStageName:str) -> List[requests.Response]:
        logger.info(f'deployment_pipelines_deploy_stage_response: {deploymentPipelineName=}, {sourceStageName=}, {targetStageName=}')
        deploymentPipelineId = self.deployment_pipelines_get_id(deploymentPipelineName=deploymentPipelineName)
        sourceStageId = self.deployment_pipelines_get_stage_id(deploymentPipelineName=deploymentPipelineName, deploymentPipelineStageName=sourceStageName) 
        targetStageId = self.deployment_pipelines_get_stage_id(deploymentPipelineName=deploymentPipelineName, deploymentPipelineStageName=targetStageName)
        body = {
            'sourceStageId': sourceStageId
            ,'targetStageId': targetStageId
        }
        response = self.request(method='post', url=f'https://api.fabric.microsoft.com/v1/deploymentPipelines/{deploymentPipelineId}/deploy', body=body)
        return response
    

    def deployment_pipelines_deploy_stage(self, deploymentPipelineName:str, sourceStageName:str, targetStageName:str) -> list:
        logger.info(f'deployment_pipelines_deploy_stage: {deploymentPipelineName=}, {sourceStageName=}, {targetStageName=}')
        response = self.response_list_unravel(self.deployment_pipelines_deploy_stage_response(deploymentPipelineName=deploymentPipelineName, sourceStageName=sourceStageName, targetStageName=targetStageName), param=None)
        return response

