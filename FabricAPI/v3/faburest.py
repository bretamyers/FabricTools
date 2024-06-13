
from _rest import FabricRestResponse


class FabricClient():
    def __init__(self) -> None:
        self.response = FabricRestResponse(method='get', url='https://api.fabric.microsoft.com/v1/admin/items')


if __name__ == '__main__':

    fr = FabricClient()

    # for _ in fr.response:
    #     print(_)

    print(fr.response)

    # aValue = [{'a': 1, 'b': 2}, {'a': 2, 'b': 2}]
    # import json
    # print(aValue)
    # print(json.loads(aValue))
    # print(json.dump(aValue))
    # print(json.dumps(aValue, default=lambda x: list(x) if isinstance(x, tuple) else str(x), indent=2))
    # print(json.dumps(aValue, indent=2))

    # print(json.dumps(aValue))
    