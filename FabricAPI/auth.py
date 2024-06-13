from azure.identity import InteractiveBrowserCredential, DeviceCodeCredential, DefaultAzureCredential
import base64
import json
import datetime
import logging

logger = logging.getLogger(__name__)
# handler = logging.StreamHandler()
# formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(name)s - %(message)s", "%Y-%m-%d %H:%M:%S")
# handler.setFormatter(formatter)
# logger.addHandler(handler)


class Interactive:
    def __init__(self):
        self.auth = InteractiveBrowserCredential()
        # self.auth = DeviceCodeCredential()
    
    """
    Get a token for a database scope
    Used to connect to a Fabric Data Wareshouse/Lakehouse SQL endpoint
    """
    def get_token_database(self) -> str:
        logger.info(f'Auth.get_token_database')
        # https://learn.microsoft.com/en-us/python/api/azure-identity/azure.identity.interactivebrowsercredential?view=azure-python
        # https://github.com/microsoft/dbt-fabric/blob/89d59caea97f9e8a7f83486f7964cd18561e0fa9/dbt/adapters/fabric/fabric_connection_manager.py#L28
        accessToken = self.auth.get_token("https://database.windows.net//.default")
        return accessToken.token


    """
    Get a auth token for the storage scope.
    Used to connect and upload data to OneLake.
    """
    def get_token_storage(self) -> str:
        logger.info(f'Auth.get_token_storage')
        # https://learn.microsoft.com/en-us/python/api/azure-identity/azure.identity.interactivebrowsercredential?view=azure-python
        accessToken = self.auth.get_token("https://storage.azure.com/.default")
        return accessToken.token
    
    
    """
    Get a auth token from the analysis (PBI) scope
    Used to connect to Fabric and run the APIs
    """
    def get_token_pbi(self) -> str:
        logger.info(f'Auth.get_token_pbi')
        accessToken = self.auth.get_token("https://analysis.windows.net/powerbi/api/.default")
        return accessToken.token
    

    """
    Get a auth token from the analysis (Fabric) scope
    Used to connect to Fabric and run the APIs. 
    #"https://api.fabric.microsoft.com/Workspace.ReadWrite.All https://api.fabric.microsoft.com/Capacity.ReadWrite.All"
    """
    def get_token_fabric(self) -> str:
        logger.info(f'Auth.get_token_fabric')
        accessToken = self.auth.get_token("https://api.fabric.microsoft.com/.default")
        return accessToken.token
    

    """
    Get a auth token from the management scope
    Used to create resources within Azure
    """
    def get_token_management(self) -> str:
        logger.info(f'Auth.get_token_management')
        accessToken = self.auth.get_token("https://management.core.windows.net/.default")
        return accessToken.token

    
    """
    Get a auth token from the key vault scope
    Used to get secrets from Azure key vault
    """
    def get_token_key_vault(self) -> str:
        logger.info(f'Auth.get_token_key_vault')
        accessToken = self.auth.get_token("https://vault.azure.net/.default")
        return accessToken.token
    
    
    """
    Create header dictionary for API requests
    """
    def create_header(self, token:str) -> dict:
        logger.info(f'Auth.create_header')
        header = {
            "Authorization": f"Bearer {token}"
            ,"Content-Type": "application/json"
        }
        return header
    

    """
    Decode an auth token
    """
    def decode_token(self, token) -> dict:
        logger.info(f'Auth.decode_token')
        # token = self.get_token_management()
        base64_meta_data = token.split(".")[1].encode("utf-8") + b'=='
        json_bytes = base64.decodebytes(base64_meta_data)
        json_string = json_bytes.decode("utf-8")
        json_dict = json.loads(json_string)
        return json_dict
    

    """
    Get the user name from an auth token
    """
    def get_username(self, token) -> str:
        logger.info(f'Auth.get_username')
        json_dict = self.decode_token(token=token)
        current_user_id = json_dict["upn"]
        return current_user_id
    

    """
    Get the expiration date (UTC) from an auth token
    If localTZ is True, then the expiration date will be converted to the local timezone
    """
    def get_token_expiration_date(self, token, localTZ:bool=None) -> str:
        logger.info(f'Auth.get_token_expiration_date')
        json_dict = self.decode_token(token=token)
        if localTZ:
            expiration_date = datetime.datetime.fromtimestamp(json_dict["exp"])
        else:
            expiration_date = datetime.datetime.utcfromtimestamp(json_dict["exp"])
        return expiration_date


    """
    Temporary method for getting a static token
    """
    def get_token_static(self) -> str:
        logger.info(f'Auth.get_token_static')
        token = ''
        return token
    

if __name__ == '__main__':

    myAuth = Interactive()
    # print(myAuth.get_token_database())
    # print(myAuth.get_token_storage())
    # print(myAuth.get_token_pbi())
    # print(myAuth.get_username(myAuth.get_token_management()))
    # print(myAuth.get_token_expiration_date(myAuth.get_token_database(), localTZ=True))
    # print(myAuth.get_token_expiration_date(myAuth.get_token_database(), localTZ=False))

    import requests
    url = 'https://api.fabric.microsoft.com/v1/worskpaces'
    response = requests.get(url=url, headers=myAuth.create_header(myAuth.get_token_pbi()))
    print(response)