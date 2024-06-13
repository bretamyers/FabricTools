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

    print(fr.deployment_pipelines_list_stages(deploymentPipelineName='Fabric Deployment'))

    # print(fr.deployment_pipelines_deploy_stage_response(deploymentPipelineName='Fabric Deployment', sourceStageName='Development', targetStageName='Test'))
    print(fr.deployment_pipelines_deploy_stage_response(deploymentPipelineName='Fabric Deployment', sourceStageName='Development', targetStageName='Test')[0].json())

