import logging
import time
from faburest import fabric_rest

logger = logging.getLogger(__name__)
# logger.setLevel(logging.INFO)

def unit_test_data_warehouse(fr:fabric_rest):

    # print(fr.workspace_list())
    
    print(fr.warehouse_list_response(workspaceName='FabricWS_Stocks_DW'))
    print(fr.warehouse_list_response(workspaceName='FabricWS_Stocks_DW')[0].json())
    print(fr.warehouse_list(workspaceName='FabricWS_Stocks_DW'))

    # # Feature is not available
    # print(fr.warehouse_get_definition(workspaceName='FabricWS_Stocks_DW', warehouseName='SampleDW'))

    # print(fr.warehouse_create_response(workspaceName='API_8e9448d9cf6f40a0b7554fa473586d28', warehouseName='DW_test_4'))
    
    # print(fr.warehouse_create_response(workspaceName='API_8e9448d9cf6f40a0b7554fa473586d28', warehouseName='DW_test_5')[0].json())

    # print(fr.warehouse_delete_response(workspaceName='API_8e9448d9cf6f40a0b7554fa473586d28', warehouseName='DW_test_5'))

    # print(fr.warehouse_update_response(workspaceName='API_8e9448d9cf6f40a0b7554fa473586d28', warehouseName='DW_test_4', warehouseNameNew='DW_test_99', warehouseDescription='This is a test warehouse'))

if __name__ == '__main__':

    sh = logging.StreamHandler()
    # sh.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
    sh.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(name)s - %(message)s", "%Y-%m-%d %H:%M:%S"))

    logger_faburest = logging.getLogger('faburest')
    logger_faburest.setLevel(logging.INFO)
    logger_faburest.addHandler(sh)


    fr = fabric_rest()

    unit_test_data_warehouse(fr)




