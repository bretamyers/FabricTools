from needlr.auth import FabricServicePrincipal, FabricInteractiveAuth
from needlr import FabricClient


auth = FabricInteractiveAuth()
fc = FabricClient(auth)

for ws in fc.workspace.ls():
    print(f"{ws.name}: Id:{ws.id} Capacity:{ws.capacityId}")
    for itm in fc.workspace.item_ls(ws.id):
        print(itm)
        # print(f"\t{itm.displayName}:{itm.type}")


