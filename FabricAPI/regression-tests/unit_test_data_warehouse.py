import logging
import time
from faburest import fabric_rest

logger = logging.getLogger(__name__)
# logger.setLevel(logging.INFO)

def unit_test_data_warehouse(fr:fabric_rest):

    # print(fr.workspace_list())
    print(fr.warehouse_list(workspaceName='FabricWS_Stocks_DW'))

    # # Feature is not available
    # print(fr.warehouse_get_definition(workspaceName='FabricWS_Stocks_DW', warehouseName='SampleDW'))

    # print(fr.warehouse_create_response(workspaceName='FabricWS_Stocks_DW', warehouseName='SampleDW_API'))

                                


if __name__ == '__main__':

    sh = logging.StreamHandler()
    sh.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))

    logger_faburest = logging.getLogger('faburest')
    logger_faburest.setLevel(logging.INFO)
    logger_faburest.addHandler(sh)


    fr = fabric_rest()

    unit_test_data_warehouse(fr)




