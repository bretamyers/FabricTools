
import workspace
import pipeline
import item
import lakehouse

class faburest():
    def __init__(self):
        self.item = item.item()
        self.workspace = workspace.workspace()
        self.pipeline = pipeline.pipeline()
        self.lakehouse = lakehouse.lakehouse()

    # def workspace(self):
    #     return workspace.workspace()
        
    # def item(self):
    #     return item.item()
    
    # def pipeline(self):
    #     return pipeline.pipeline()


if __name__ == '__main__':

    # workspaceName = workspace.workspace().get_name()
    workspaceName = faburest().workspace.get_name()
    print(workspaceName)
    print(dir(workspaceName))
