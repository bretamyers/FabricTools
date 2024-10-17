from faburest import fabric_rest
import json, logging

logger = logging.getLogger(__name__)


if __name__ == '__main__':

    sh = logging.StreamHandler()
    # sh.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
    sh.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(name)s - %(message)s", "%Y-%m-%d %H:%M:%S"))

    logger_faburest = logging.getLogger('faburest')
    logger_faburest.setLevel(logging.INFO)
    logger_faburest.addHandler(sh)

    fr = fabric_rest()

    # print(fr.connection_list_response()[0].json())
    # for _ in fr.connection_list_response():
    #     print(json.dumps(_.json(), indent=2))

    # for connection in fr.connection_list():
    #     # print(connection)

    print(fr.connection_get_object(connectionName='dtests'))
    print(fr.connection_get_id(connectionName='dtests'))



