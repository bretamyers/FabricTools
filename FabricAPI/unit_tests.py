import logging
import time
# from FabricAPI import faburest
# import sys, os
# SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
# sys.path.append(os.path.dirname(SCRIPT_DIR))
from faburest import fabric_rest

logger = logging.getLogger(__name__)


def unit_test_workspace(fr:fabric_rest):
    workspaceName = 'WS_UnitTest_Workspace'
    # create workspace
    fr.workspace_create(workspaceName=workspaceName, capacityName='fabricbamdemo')
    # check workspace exists (list workspace)
    fr.workspace_get_id(workspaceName=workspaceName)
    # rename workspace
    fr.workspace_rename(workspaceName=workspaceName, workspaceNameTarget='WS_UnitTest_Workspace_Renamed', description='Test Description')
    # check workspace exists (list workspace)
    # update workspace
    fr.workspace_assign_capacity(workspaceName=workspaceName, capacityName='fabricbamtemp')
    # check workspace updated
    fr.workspace_get_id(workspaceName=workspaceName)
    
    # cleanup artifacts
    # fr.workspace_delete(workspaceName=workspaceName)
    print('unit_test_lakehouse completed')


def unit_test_pipeline():
    # create pipeline
    # check pipeline exists (list pipeline)
    # rename pipeline
    # check pipeline exists (list pipeline)
    # update pipeline
    # check pipeline updated
    # clone pipeline
    # check pipeline exists (list pipeline)
    # delete pipeline clone
    # check pipeline is deleted (list pipeline)
    # run pipeline
    # check pipeline run status
    # cancel pipeline run
    pass


def unit_test_notebook():
    # create notebook
    # check notebook exists (list notebook)
    # rename notebook
    # check notebook exists (list notebook)
    # update notebook
    # check notebook updated
    # delete notebook
    # check notebook is deleted (list notebook)
    # run notebook
    # check notebook run status
    # cancel notebook run
    pass


def unit_test_lakehouse(fr:fabric_rest):
    workspaceName = 'WS_UnitTest_Lakehouse'
    lakehouseName = 'LH_UnitTest_Lakehouse'
    # create workspace
    fr.workspace_create(workspaceName=workspaceName, capacityName='fabricbamdemo')
    # create lakehouse
    fr.lakehouse_create(workspaceName=workspaceName, lakehouseName=lakehouseName)
    # check lakehouse exists (list lakehouse)
    fr.lakehouse_get_object(workspaceName=workspaceName, lakehouseName=lakehouseName)
    # rename lakehouse
    # fr.lakheouse_re
    # check lakehouse exists (list lakehouse)
    # update lakehouse
    # check lakehouse updated
    # delete lakehouse
    # check lakehouse is deleted (list lakehouse)
    # create lakehouse shortcut
    # check lakehouse shortcut exists (list lakehouse)

    # cleanup artifacts
    # fr.workspace_delete(workspaceName=workspaceName)
    print('unit_test_lakehouse completed')


def unit_test_scale():
    # create 100 workspaces
    # create 100 pipelines in each workspace
    # list all items
    # delete all items
    # delete all workspaces
    pass


# TODO - needs some additional work as we need to get the user id from entra
def unit_test_users():
    pass


def unit_test_devops():
    # create workspace - WS_Dev
    fabric_rest().workspace_create(workspaceName='WS_Dev')
    # assign a fabric capacity to workspace - WS_Dev
    fabric_rest().workspace_assign_capacity(workspaceName='WS_Dev', capacityName='fabricbamdemo')
    # add users to workspace - WS_Dev
    # fabric_rest().workspace_add_role_assignment(workspaceName='WS_Dev', principalName='admin', role='Admin')
    # create pipeline - Create pipeline in WS_Dev
    # pipelineDefinition = '' # read from file
    # fabric_rest().pipeline_create(workspaceName='WS_Dev', pipelineName='PL_Simple', pipelineDefinition=pipelineDefinition)
    # create notebook - Create notebook in WS_Dev
    # notebookDefinition = '' # read from file
    # fabric_rest().notebook_create(workspaceName='WS_Dev', notebookName='NB_Simple', notebookDefinition=notebookDefinition)
    # create lakehouse - Create lakehouse in WS_Dev
    # fabric_rest().lakehouse_create(workspaceName='WS_Dev', lakehouseName='LH_Dev')
    # create shortcut files - Create shortcut files in WS_Dev
    # create shortcut tables - Create shortcut tables in WS_Dev
    # create semantic model - Create semantic model in WS_Dev
    # fabric_rest().semantic_model_create(workspaceName='WS_Dev', semanticModelName='SM_Dev')

    # create workspace - WS_Test
    # add users to workspace - WS_Test
    # clone pipeline - WS_Dev to WS_Test
    # clone notebook - WS_Dev to WS_Test
    # clone lakehouse - WS_Dev to WS_Test
    # clone semantic model - WS_Dev to WS_Test - decode, change connect to new lakehouse, encode, deploy
    pass



if __name__ == '__main__':
    
    # https://stackoverflow.com/questions/7016056/python-logging-not-outputting-anything
    logger.addHandler(logging.StreamHandler())
    logger.setLevel(logging.INFO)

    fr = fabric_rest()

    unit_test_lakehouse(fr=fr)

    ## Items
    # print(fabric_rest().item_list(workspaceName='WS_Steve'))
    # print(fabric_rest().item_get_definition(workspaceName='WS_Steve', itemName='NB_API_ClonePipeline', itemType='DataPipeline'))

    # format='ipynb'

    # print(fabric_rest().capacity_list())
    # print(fabric_rest().principal_list(prefix='brmyers@bamsql.com'))
    # print(fabric_rest().principal_list(prefix='sp_bam'))
    # print(fabric_rest().principal_get_id(principalName='brmyers@bamsql.com'))

    ## Workspaces
    # print(fabric_rest().workspace_create(workspaceName='WS_API_Created_2', capacityName='fabricbamdemo'))
    # print(fabric_rest().workspace_delete(workspaceName='WS_API_Created'))
    # print(fabric_rest().workspace_list_response())
    # print(fabric_rest().workspace_list())
    # print(fabric_rest().workspace_get_id(workspaceName='WS_Steve'))
    # print(fabric_rest().workspace_get_access_details_response(workspaceName='WS_Steve'))
    # print(fabric_rest().workspace_get_access_details(workspaceName='WS_Steve'))
    # print(fabric_rest().workspace_get_access_details_user(userName='sp_bam', workspaceName='WS_Steve'))
    # print(json.dumps(fabric_rest().workspace_get_access_details_user(userName='sp_bam'), indent=4))
    # print(json.dumps(fabric_rest().workspace_get_access_details_user(userName='admin MCAPS'), indent=4))
    # print(json.dumps(fabric_rest().workspace_get_access_details_user(userName='Shane Ochotny'), indent=4))
    # print(fabric_rest().workspace_add_role_assignment(workspaceName='WS_API_Code', principalName='brmyers@bamsql.com', role='Contributor'))


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
    # print(fabric_rest().lakehouse_get_properties(workspaceName='WS_Steve', lakehouseName='LH_InternetSales'))
    # Is not supported yet
    # print(fabric_rest().lakehouse_update(workspaceName='WS_Steve', lakehouseName='LH_Test2', lakehouseNameNew='LH_Test3', lakehouseDescription='test description'))
    # print(fabric_rest().lakehouse_get_tables(workspaceName='WS_Steve', lakehouseName='LH_InternetSales'))

    ## TODO - Shortcut creation has some complexitites to it. 
    ## Need to think through various scenarios to see what makes sense in the short term and long term.
    # print(fabric_rest().lakehouse_create_shortcut_adls(workspaceName='WS_Steve', itemName='', shortcutName='', shortcutPath='', adlsPath='', adlsSubPath=''))
    
    
    ## Pipelines
    # print(fabric_rest().pipeline_list_response(workspaceName='WS_Steve'))
    # print(fabric_rest().pipeline_list(workspaceName='WS_Steve'))
    # print(fabric_rest().pipeline_get_definition(workspaceName='WS_Steve', pipelineName='PL_Simple_Updated'))
    # # Create Pipeline
    # myPipelineDefinition = fabric_rest().pipeline_get_definition(workspaceName='WS_Steve', pipelineName='PL_Simple5')
    # print(myPipelineDefinition)
    # print(fabric_rest().pipeline_create(workspaceName='WS_Steve', pipelineName='PL_Simple5_API_Created2', pipelineDefinition=myPipelineDefinition))

    # import _util
    # myPipelineDefinition = fabric_rest().pipeline_get_definition(workspaceName='WS_Steve', pipelineName='PL_Simple5')
    # print(_util._base64_decode(myPipelineDefinition['definition']['parts'][0]['payload']))
    
    
    # print(fabric_rest().pipeline_get_definition_parts(workspaceName='WS_Steve', pipelineName='PL_Simple'))
    # print(fabric_rest().pipeline_clone(workspaceNameSource='WS_Steve', pipelineNameSource='PL_Simple5_API_Created2', workspaceNameTarget='WS_Steve', pipelineNameTarget='PL_Simple5_API_Created5'))
    # print(fabric_rest().pipeline_update_metadata(workspaceName='WS_Steve', pipelineName='PL_Simple_Updated', displayName="PL_Simple", description=""))
    # print(fabric_rest().pipeline_delete(workspaceName='WS_Steve', pipelineName='PL_Simple5_API_Created'))
    # print(fabric_rest().pipeline_run(workspaceName='WS_Steve', pipelineName='PL_Simple5'))
    # pipelineRunId = fabric_rest().pipeline_run(workspaceName='WS_Steve', pipelineName='PL_Simple5')
    # print(fabric_rest().pipeline_get_run_instance(workspaceName='WS_Steve', pipelineName='PL_Simple5', jobInstanceId='b40bb47f-e9ae-4ae9-936a-a110d0d971bc')) #pipelineRunId))
    # pipelineRunId = fabric_rest().pipeline_run(workspaceName='WS_Steve', pipelineName='PL_Simple5')
    # while True:
    #     time.sleep(5)
    #     pipelineRunStatus = fabric_rest().pipeline_get_run_instance(workspaceName='WS_Steve', pipelineName='PL_Simple5', jobInstanceId=pipelineRunId)
    #     print(pipelineRunStatus)
    #     # if pipelineRunStatus['status'] == 'Completed':
    #     if pipelineRunStatus['status'] == 'InProgress':
    #         break
    # # time.sleep(30)
    # print(fabric_rest().pipeline_cancel_run_instance(workspaceName='WS_Steve', pipelineName='PL_Simple5', jobInstanceId=pipelineRunId))


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
    
    
    