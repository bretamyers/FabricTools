
import _restclient 
import faburest
import requests
import logging
from typing import Literal

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

class item(faburest.faburest):
    def __init__(self, header:str=None):
        if header is None:
            self.header = _restclient.rest().header
        else:    
            self.header = header

        # self.id = .test
        super().get_workspace()


    def test(self):
        # t = self.
        return None
    
    # # https://learn.microsoft.com/en-us/rest/api/fabric/admin/items/list-items?tabs=HTTP
    # def get_response(self, workspaceName:str, itemType:str=None) -> requests.Response:
    #     workspaceId = self.workspace_get_id(workspaceName=workspaceName)
    #     response = self.request(method='get', url=f'https://api.fabric.microsoft.com/v1/workspaces/{workspaceId}/items{f'?type={itemType}' if itemType else ''}')
    #     return response


    # def list(self, workspaceName:str, itemType:str=None) -> list:
    #     item_get_response = self.item_get_response(workspaceName=workspaceName, itemType=itemType)
    #     item_list = item_get_response.json().get('value')
    #     return item_list


    # def get_object(self, workspaceName:str, itemName:str, itemType:str=None) -> dict:
    #     item_list = self.item_list(workspaceName=workspaceName, itemType=itemType)
    #     artifact = [item for item in item_list if item.get('displayName') == itemName][0]
    #     return artifact


    # def get_id(self, workspaceName:str, itemName:str, itemType:str=None) -> str:
    #     artifactObject = self.item_get_object(workspaceName=workspaceName, itemName=itemName, itemType=itemType)
    #     artifactId = artifactObject.get('id')
    #     return artifactId


    # def get_definition_response(self, workspaceName:str, itemName:str, itemType:str=None, format=None) -> requests.Response:
    #     workspaceId = self.workspace_get_id(workspaceName=workspaceName)
    #     itemId = self.item_get_id(workspaceName=workspaceName, itemName=itemName, itemType=itemType)
    #     # logger.info(f'item_get_definition_response {workspaceName=}:{workspaceId=} - {itemName=}:{itemId}')
    #     response = self.request(method='post', url=f'https://api.fabric.microsoft.com/v1/workspaces/{workspaceId}/items/{itemId}/getDefinition{f'?format={format}' if format else ''}')
    #     return response
    

    # def get_definition(self, workspaceName:str, itemName:str, itemType:str='', format=None) -> dict:
    #     response = self.item_get_definition_response(workspaceName=workspaceName, itemName=itemName, itemType=itemType).json()
    #     return response
    

    # def get_definition_parts(self, workspaceName:str, itemName:str, itemType:str='', format=None) -> list:
    #     response = self.item_get_definition(workspaceName=workspaceName, itemName=itemName, itemType=itemType, format=format).get('definition').get('parts')
    #     return response


    # def create(self, workspaceName:str, itemName:str, itemType:str, itemDefinition:dict=None) -> requests.Response:
    #     workspaceId = self.workspace_get_id(workspaceName=workspaceName)
    #     body = {"displayName": itemName
    #             ,"type": itemType
    #             ,**(itemDefinition if itemDefinition is not None else {})
    #             # ,**({'definition': itemDefinition.get('definition')} if itemDefinition is not None else {}) ## This can be None when creating a Lakehouse
    #             }
    #     response = self.request(method='post', url=f'https://api.fabric.microsoft.com/v1/workspaces/{workspaceId}/items', body=body)
    #     return response
    

    # # This one does not work yet!! (for lakehouse)
    # def delete(self, workspaceName:str, itemName:str):
    #     workspaceId = self.workspace_get_id(workspaceName=workspaceName)
    #     itemId = self.item_get_id(workspaceName=workspaceName, itemName=itemName)
    #     # logger.info(f'item_delete - {workspaceName=}:{workspaceId=} - {itemName=}:{itemId}')
    #     url = f'https://api.fabric.microsoft.com/v1/workspaces/{workspaceId}/items/{itemId}'
    #     response = self.request(method='delete', url=f'https://api.fabric.microsoft.com/v1/workspaces/{workspaceId}/items/{itemId}')
    #     return response


    # def update_metadata(self, workspaceName:str, itemName:str, body:dict) -> str:
    #     workspaceId = self.workspace_get_id(workspaceName=workspaceName)
    #     itemId = self.item_get_id(workspaceName=workspaceName, itemName=itemName)
    #     response = self.request(method='patch', url=f'https://api.fabric.microsoft.com/v1/workspaces/{workspaceId}/items/{itemId}', body=body)
    #     return response
    

    # ## TODO WIP
    # def update_definition(self, workspaceName:str, itemName:str, definition:dict) -> str:
    #     workspaceId = self.workspace_get_id(workspaceName=workspaceName)
    #     itemId = self.item_get_id(workspaceName=workspaceName, itemName=itemName)
    #     response = self.request(method='post', url=f'https://api.fabric.microsoft.com/v1/workspaces/{workspaceId}/items/{itemId}/updateDefinition', body=definition)
    #     return response


    # #
    # #   jobType = ['Pipeline']
    # #
    # def run_job(self, workspaceName:str, itemName:str, jobType:Literal['Pipeline']) -> requests.Response:
    #     workspaceId = self.workspace_get_id(workspaceName=workspaceName)
    #     itemId = self.item_get_id(workspaceName=workspaceName, itemName=itemName)
    #     # response = self.request(method='post', url=f'https://api.fabric.microsoft.com/v1/workspaces/{workspaceId}/items/{itemId}/jobs/instances{f'?jobType={jobType}' if jobType else ''}')
    #     # print(f'{response=}')
    #     # print(f'{response.headers=}')
    #     ## exception because in preview, the job instance id is found in the response header and not body so we have to use requests directly.
    #     logger.info('item_run_job')
    #     response = requests.request(method='post', url=f'https://api.fabric.microsoft.com/v1/workspaces/{workspaceId}/items/{itemId}/jobs/instances{f'?jobType={jobType}' if jobType else ''}', headers=self.header)
    #     return response.headers.get('Location').split('jobs/instances/')[-1]
    

    # def get_job_instance_response(self, workspaceName:str, itemName:str, jobInstanceId:str) -> requests.Response:
    #     workspaceId = self.workspace_get_id(workspaceName=workspaceName)
    #     itemId = self.item_get_id(workspaceName=workspaceName, itemName=itemName)
    #     response = self.request(method='get', url=f'https://api.fabric.microsoft.com/v1/workspaces/{workspaceId}/items/{itemId}/jobs/instances/{jobInstanceId}')
    #     return response
    
    
    # def get_job_instance(self, workspaceName:str, itemName:str, jobInstanceId:str) -> dict:
    #     response = self.item_get_job_instance_response(workspaceName=workspaceName, itemName=itemName, jobInstanceId=jobInstanceId).json()
    #     return response
    
    
    # def cancel_job_instance(self, workspaceName:str, itemName:str, jobInstanceId:str) -> requests.Response:
    #     workspaceId = self.workspace_get_id(workspaceName=workspaceName)
    #     itemId = self.item_get_id(workspaceName=workspaceName, itemName=itemName)
    #     response = self.request(method='post', url=f'https://api.fabric.microsoft.com/v1/workspaces/{workspaceId}/items/{itemId}/jobs/instances/{jobInstanceId}/cancel')
    #     return response
    