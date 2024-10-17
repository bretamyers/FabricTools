from faburest import fabric_rest
import logging

if __name__ == '__main__':

    sh = logging.StreamHandler()
    # sh.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
    sh.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(name)s - %(message)s", "%Y-%m-%d %H:%M:%S"))

    logger_faburest = logging.getLogger('faburest')
    logger_faburest.setLevel(logging.INFO)
    logger_faburest.addHandler(sh)

    fr = fabric_rest()
 


    import json
    itemList = fr.capacity_list()
    print(json.dumps(itemList, indent=2))


    capacityIdList = set()
    for workspace in fr.workspace_list():
        capacityIdList.add(workspace.get('capacityId', None))

    print(capacityIdList)
    for capacity in capacityIdList:
        # print(capacity)
        for cap in itemList:
            if cap.get('id', '') == capacity:
                print(cap)
                
