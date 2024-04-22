
import workspace
import pipeline
import item
import lakehouse
import _util, _restclient

class faburest:
    def __init__(self):
        self.token_pbi = _util._get_token_cached() #TODO: change to pbi token
        self.token_fabric = _util._get_token_cached() #TODO: change to fabric token
        self.header_pbi = _restclient.rest().create_header(audience='pbi')
        self.header_fabric = _restclient.rest().create_header(audience='fabric')

        self.item = item.item(self.header_fabric)
        self.workspace = workspace.workspace(self.header_fabric)
        # self.pipeline = pipeline.pipeline(self.header_fabric)
        # self.lakehouse = lakehouse.lakehouse(self.header_fabric)


    def get_workspace(self):
        return self.workspace
    
    
    def download_workspace_artifacts(self, workspaceName:str) -> None:
        raise NotImplementedError
    

    def clone_workspace(self, workspaceNameSource:str, workspaceNameTarget:str) -> None:
        raise NotImplementedError
    


if __name__ == '__main__':

    fb = faburest()
    # print(fb.item.get_response('name'))

    # workspaceName = workspace.workspace().get_name()
    # workspaceName = faburest().workspace.get_name()
    # print(workspaceName)
    # print(dir(workspaceName))
