
from _rest import FabricRestResponse


class FabricClient():
    def __init__(self) -> None:
        self.response = FabricRestResponse(method='get', url='https://api.fabric.microsoft.com/v1/admin/items')


if __name__ == '__main__':

    fr = FabricClient()

    # for _ in fr.response:
    #     print(_)

    print(fr.response)
