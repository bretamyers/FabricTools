import logging
# from FabricAPI import faburest
# import sys, os
# SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
# sys.path.append(os.path.dirname(SCRIPT_DIR))
from faburest import fabric_rest

logger = logging.getLogger(__name__)

if __name__ == '__main__':
    
    # https://stackoverflow.com/questions/7016056/python-logging-not-outputting-anything
    logger.addHandler(logging.StreamHandler())
    logger.setLevel(logging.INFO)


    ## Items
    # print(fabric_rest().item_list(workspaceName='WS_Steve'))
    # print(fabric_rest().item_get_definition(workspaceName='WS_Steve', itemName='NB_API_ClonePipeline', itemType='DataPipeline'))

    # format='ipynb'

    ## Workspaces
    # print(fabric_rest().workspace_list_response())
    # print(fabric_rest().workspace_list())
    # print(fabric_rest().workspace_get_id(workspaceName='WS_Steve'))
    # print(fabric_rest().workspace_get_access_details_response(workspaceName='WS_Steve'))
    # print(fabric_rest().workspace_get_access_details(workspaceName='WS_Steve'))
    # print(fabric_rest().workspace_get_access_details_user(userName='sp_bam', workspaceName='WS_Steve'))
    # print(json.dumps(fabric_rest().workspace_get_access_details_user(userName='sp_bam'), indent=4))
    # print(json.dumps(fabric_rest().workspace_get_access_details_user(userName='admin MCAPS'), indent=4))
    # print(json.dumps(fabric_rest().workspace_get_access_details_user(userName='Shane Ochotny'), indent=4))
    


    ## Users
    ## This does not work yet as we need a way to to get the user id from entra
    # print(fabric_rest().user_get_access_entities(userName='a4afe56e-0589-4c74-81b1-f9928ad84d30'))


    ## Lakehouse
    # print(fabric_rest().lakehouse_get_id(workspaceName='WS_Steve', lakehouseName='LH_InternetSales'))
    # print(fabric_rest().lakehouse_get_object(workspaceName='WS_Steve', lakehouseName='LH_InternetSales'))
    # print(fabric_rest().lakehouse_get_definition_response(workspaceName='WS_Steve', lakehouseName='LH_InternetSales'))
    # print(fabric_rest().lakehouse_get_definition_parts(workspaceName='WS_Steve', lakehouseName='LH_InternetSales'))
    # print(fabric_rest().lakehouse_create(workspaceName='WS_Steve', lakehouseName='LH_Test2'))
    # print(fabric_rest().lakehouse_delete(workspaceName='WS_Steve', lakehouseName='LH_Test2'))
    
    ## TODO - Shortcut creation has some complexitites to it. 
    ## Need to think through various scenarios to see what makes sense in the short term and long term.
    # print(fabric_rest().lakehouse_create_shortcut_adls(workspaceName='WS_Steve', itemName='', shortcutName='', shortcutPath='', adlsPath='', adlsSubPath=''))
    
    
    ## Pipelines
    # print(fabric_rest().pipeline_list_response(workspaceName='WS_Steve'))
    # print(fabric_rest().pipeline_list(workspaceName='WS_Steve'))
    # print(fabric_rest().pipeline_get_definition(workspaceName='WS_Steve', pipelineName='PL_Simple_Updated'))
    # # Create Pipeline
    # myPipelineDefinition = fabric_rest().pipeline_get_definition(workspaceName='WS_Steve', pipelineName='PL_Simple5')
    # # print(myPipelineDefinition)
    # print(fabric_rest().pipeline_create(workspaceName='WS_Steve', pipelineName='PL_Simple5_API_Created2', pipelineDefinition=myPipelineDefinition))

    # print(fabric_rest().pipeline_get_definition_parts(workspaceName='WS_Steve', pipelineName='PL_Simple'))
    # print(fabric_rest().pipeline_clone(workspaceNameSource='WS_Steve', pipelineNameSource='PL_Simple5_API_Created2', workspaceNameTarget='WS_Steve', pipelineNameTarget='PL_Simple5_API_Created5'))
    # print(fabric_rest().pipeline_update_metadata(workspaceName='WS_Steve', pipelineName='PL_Simple_Updated', displayName="PL_Simple", description=""))
    print(fabric_rest().pipeline_delete(workspaceName='WS_Steve', pipelineName='PL_Simple5_API_Created'))


    ## Notebooks
    # print(fabric_rest().notebook_get_item_definition(workspaceName='WS_Steve', notebookName='NB_Simple'))
    # print(fabric_rest().notebook_delete(workspaceName='WS_Steve', notebookName='Notebook 3'))


    ## Capacities - Unoffical APIs
    # print(fabric_rest().capacity_list_response())
    # print(fabric_rest().capacity_list())
    # print(fabric_rest().capacity_get(capacityName='fabricbamdemo'))
    

    ## Extensions
    # print(fabric_rest().download_workspace_items(workspaceName="", outputFolderPath=""))


    # workspaceId = '372dfc2d-e201-49d7-a28b-7cfc015a9317'
    # itemId = '00fb81dc-6133-45f2-9ac1-e524eac5a47f'
    # response = requests.delete(url=f'https://api.fabric.microsoft.com/v1/workspaces/{workspaceId}/items/{itemId}', headers=fabric_rest().header)
    # print(response.status_code)
    # print(response.json()) # failed because there is no body
    # print(response.headers)
    
    
    