from faburest import fabric_rest
import logging
import concurrent.futures
import random, string, datetime

def unit_test_pagination(fr:fabric_rest):
    notebook_get_definition = fr.notebook_get_item_definition(workspaceName='WS_Audit_All_Activities', notebookName='Notebook 2')
    print(notebook_get_definition)

    workspaceNameTarget = 'WS_Audit_All_Activities'
    for _ in range(460, 10000):
        # print(_)
        notebookNameTarget = f'Notebook API {_}'
        response = fr.item_create(workspaceName=workspaceNameTarget, itemName=notebookNameTarget, itemType='Notebook', itemDefinition=notebook_get_definition)
        # fr.notebook_clone(workspaceNameSource=workspaceNameTarget, notebookNameSource='Notebook 2', workspaceNameTarget=workspaceNameTarget, notebookNameTarget=notebookNameTarget)

    # pass



def unit_test_pagination_threading(fr:fabric_rest, prefix:str, notebook_get_definition):
    workspaceNameTarget = 'WS_Audit_All_Activities'
    # for _ in range(460, 10000):
    # print(_)
    notebookNameTarget = f'Notebook API {prefix}'
    response = fr.item_create(workspaceName=workspaceNameTarget, itemName=notebookNameTarget, itemType='Notebook', itemDefinition=notebook_get_definition)
    # fr.notebook_clone(workspaceNameSource=workspaceNameTarget, notebookNameSource='Notebook 2', workspaceNameTarget=workspaceNameTarget, notebookNameTarget=notebookNameTarget)
    return response
    # pass


if __name__ == '__main__':

    sh = logging.StreamHandler()
    # sh.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
    sh.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(name)s - %(message)s", "%Y-%m-%d %H:%M:%S"))

    logger_faburest = logging.getLogger('faburest')
    logger_faburest.setLevel(logging.INFO)
    logger_faburest.addHandler(sh)

    fr = fabric_rest()
    # unit_test_pagination(fr)

    # print(len(fr.item_get_response(workspaceName='WS_Audit_All_Activities')))


    # startLen = len(fr.item_list(workspaceName='WS_Audit_All_Activities'))
    # print(f'{startLen=}')

    # workspaceItemList = fr.item_list(workspaceName='WS_Audit_All_Activities')
    # # print(workspaceList)
    # print(len(workspaceItemList))


    # itemList = fr.item_list_admin()
    # # print(workspaceList)
    # print(len(itemList))
    # print(itemList)


    import json
    itemList = fr.capacity_list()
    print(json.dumps(itemList, indent=2))

    # # print(json.dumps(fr.warehouse_list(workspaceName='WS_Demo_InternetSales'), indent=2))
    # print(json.dumps(fr.lakehouse_list(workspaceName='WS_Demo_InternetSales'), indent=2))
    # # Database = 7da1ae2b-fe82-4040-b51e-62b8398b6b0d
    # print(json.dumps(fr.sqlendpoint_list(workspaceName='WS_Demo_InternetSales'), indent=2))

    capacityIdList = set()
    for workspace in fr.workspace_list():
        capacityIdList.add(workspace.get('capacityId', None))

    print(capacityIdList)
    for capacity in capacityIdList:
        # print(capacity)
        for cap in itemList:
            if cap.get('id', '') == capacity:
                print(cap)
                

    # print(fr.capacity_get(capac='1d2b3c4d-5e6f-7g8h-9i0j-1k2l3m4n5o6p'))


    ## Using Threading
    ## Create a bunch of notebook items
    # notebook_get_definition = fr.notebook_get_item_definition(workspaceName='WS_Audit_All_Activities', notebookName='Notebook 2')
    # futuresSet = set()
    # cnt = 0
    # notebookCreateCnt = 2500
    # threadCnt = 5
    # startTime = datetime.datetime.now()
    # with concurrent.futures.ThreadPoolExecutor(max_workers=threadCnt) as executor:
    #     for i in range(threadCnt):
    #         cnt += 1
    #         rand = ''.join(random.choices(string.ascii_letters + string.digits, k=5))
    #         futuresSet.add(executor.submit(unit_test_pagination_threading, fr, f'{rand}', notebook_get_definition))

    #     while len(futuresSet) > 0: # this is needed as it appears that as_completed is static
    #         for executionFuture in concurrent.futures.as_completed(futuresSet):
    #             # executionFuture.result() # Not needed
    #             cnt += 1
    #             futuresSet.remove(executionFuture)
    #             if cnt < notebookCreateCnt+1:
    #                 rand = ''.join(random.choices(string.ascii_letters + string.digits, k=5))
    #                 futuresSet.add(executor.submit(unit_test_pagination_threading, fr, f'1_{rand}', notebook_get_definition))
    #                 break # need to break to utilize the as_completed function. Is there a better way?



    # print(f"{startLen=}' : endLen={len(fr.item_list(workspaceName='WS_Audit_All_Activities'))} : {(datetime.datetime.now()-startTime).total_seconds()}")


