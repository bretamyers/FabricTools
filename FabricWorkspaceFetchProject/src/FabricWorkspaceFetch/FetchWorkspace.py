import Authentication
import FabricResponse

def fetch_workspace(workspace_name:str):
    print(f"Fetching workspace {workspace_name}")
    Authentication.FabricAuthentication()
    

if __name__ == "__main__":
    fetch_workspace('myTest')

