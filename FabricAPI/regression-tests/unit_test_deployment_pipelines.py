from faburest import fabric_rest
import logging
import concurrent.futures
import random, string, datetime


if __name__ == '__main__':

    sh = logging.StreamHandler()
    # sh.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
    sh.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(name)s - %(message)s", "%Y-%m-%d %H:%M:%S"))

    logger_faburest = logging.getLogger('faburest')
    logger_faburest.setLevel(logging.INFO)
    logger_faburest.addHandler(sh)

    fr = fabric_rest()
   
    print(fr.deployment_pipelines_list())

    # print(fr.deployment_pipelines_list_stages(deploymentPipelineName='Fabric Deployment'))

    deployment = fr.deployment_pipelines_deploy_stage_response(deploymentPipelineName='Fabric Deployment'
                                                               , sourceStageName='Development'
                                                               , targetStageName='Test')
    print(deployment)
    print(deployment[0].json())



    # for _ in fr.item_list_admin():
    #     print(_)


    # Using Databricks today and shortcuting to delta tables.
    # (1) Data Warehouse, (2) Lakehouse (shortcuts and tables), Pipelines, (3) Semantic Model, Notebooks



