
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


if __name__ == '__main__':

    # workspaceName = workspace.workspace().get_name()
    workspaceName = faburest().workspace.get_name()
    print(workspaceName)
    print(dir(workspaceName))
