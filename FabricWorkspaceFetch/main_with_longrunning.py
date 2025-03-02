# from msal import PublicClientApplication
import msal
import json, requests
import os, time

class FabricResponse():
    def __init__(self, response:requests.Response):
        self.status_code = response.status_code
        self.headers = response.headers
        self.location = self.headers["Location"] if "Location" in self.headers else None
        self.is_accepted = response.status_code == 202
        self.is_successful = 200 <= response.status_code < 400
        self.method = response.request.method

        self.body = None
        try:
            response.raise_for_status()
            if response.status_code != 204 and response.text and response.text != "":
                if "responseNotJson" in kwargs and kwargs["responseNotJson"]:
                    self.body = response.content
                else:
                    self.body = json.loads(response.text)
        except json.JSONDecodeError:
            raise ValueError("Error in parsing: {}".format(response.text))
        except requests.RequestException:
            if "error" in response.text:
                raise FabricException(response.text)
            else:
                raise requests.RequestException(response.text)


def request_call(method, url, headers) -> requests.Response:
    response = requests.request(method=method, url=url, headers=headers)
    return FabricResponse(response)


def request_long_running_call(method, url, headers) -> requests.Response:
    retry_attempts = 5
    response = request_call(method=method, url=url, headers=headers)

    # If inital response is accepted
    if response.status_code == 202:
        print("Accepted")
        _completed = False
        _retry_after = int(response.headers["Retry-After"] if "Retry-After" in response.headers else None)
        _result = {}
        for _ in range(retry_attempts):
            time.sleep(_retry_after)
            op_status = request_call(
                method='get',
                url=response.headers["Location"] if "Location" in response.headers else None,
                headers=headers
            )
            # If it's a non-terminal state, keep going
            if op_status.json()["status"] in ["NotStarted", "Running", "Undefined"]:
                _retry_after = int(op_status.headers["Retry-After"] if "Retry-After" in response.headers else None)
                continue
            # Successful state, get the response from the Location header
            elif op_status.json()["status"] == "Succeeded":
                _completed = True
                op_status_location = op_status.headers["Location"] if "Location" in response.headers else None
                # If the operation is complete, but there is no location provided, then return the last requested payload.
                if op_status_location is None:
                    _result = op_status
                else:
                    op_results = request_call(
                        method='get',
                        url=op_status_location,
                        headers=headers
                    )
                    _result = op_results
                break
            # Unhandled but terminal state
            else:
                _completed = True
                _result = op_status
                break
        # Fall through
        if _completed:
            return _result
        else:
            raise ValueError("Operation did not complete in time.")
    else:
        print(response.status_code)
        return response


DEVELOPER_SIGN_ON_CLIENT_ID = "04b07795-8ddb-461a-bbee-02f9e1bf7b46"
workspaceName = 'WS_Demo_InternetSales'

# cred = msal.PublicClientApplication(client_id=DEVELOPER_SIGN_ON_CLIENT_ID, authority="https://login.microsoftonline.com/organizations")
# token = cred.acquire_token_interactive(scopes=["https://api.fabric.microsoft.com/.default"])

token = {'access_token': 'eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsIng1dCI6ImltaTBZMnowZFlLeEJ0dEFxS19UdDVoWUJUayIsImtpZCI6ImltaTBZMnowZFlLeEJ0dEFxS19UdDVoWUJUayJ9.eyJhdWQiOiJodHRwczovL2FwaS5mYWJyaWMubWljcm9zb2Z0LmNvbSIsImlzcyI6Imh0dHBzOi8vc3RzLndpbmRvd3MubmV0L2E1M2M3NDZmLTc5YTctNGVkOC1hNjRiLWJiZjEzYTNiZGE2YS8iLCJpYXQiOjE3NDA3Nzg1NTIsIm5iZiI6MTc0MDc3ODU1MiwiZXhwIjoxNzQwNzgzNTYwLCJhY2N0IjowLCJhY3IiOiIxIiwiYWlvIjoiQWFRQVcvOFpBQUFBMlBMbWF3WThWRk9kNnIyendJajRobTlVS2pOMjdqRVJPd3k1MmVBMHA3cFFvR1FlSy9oMUMyVzVHRVNzYThZT1FmWFNBWjU5MVZFcjFSQjlGQStvcFRmcWVzY0FQNVRwdnRxYUM0K3VyWDZxWFR2MWZZQ3pOZ2FaaVdLQVkwNHNtRzRRVkd2VVp6eU50U1k4UTd6R1NHSGpkTlZMTlJ4S1loS3dDS3BqdkVWb0hGK1dwMVVSSGNXZmdpbDkzRTJIRW5QL2JJM0YzTVhQamJvY2xlZmk2UT09IiwiYW1yIjpbInB3ZCIsImZpZG8iLCJtZmEiXSwiYXBwaWQiOiIwNGIwNzc5NS04ZGRiLTQ2MWEtYmJlZS0wMmY5ZTFiZjdiNDYiLCJhcHBpZGFjciI6IjAiLCJmYW1pbHlfbmFtZSI6Ik15ZXJzIiwiZ2l2ZW5fbmFtZSI6IkJyZXQiLCJpZHR5cCI6InVzZXIiLCJpcGFkZHIiOiI0Ny4yMjUuMC4xNjQiLCJuYW1lIjoiQnJldCIsIm9pZCI6ImU0Nzc3MTg5LTQ2ZjktNGYyOC1hZDgwLTA5NjVmMTcwMDQ0NiIsInB1aWQiOiIxMDAzMjAwM0U1NTM0Q0REIiwicmgiOiIxLkFiY0FiM1E4cGFkNTJFNm1TN3Z4T2p2YWFna0FBQUFBQUFBQXdBQUFBQUFBQUFEOEFQNjNBQS4iLCJzY3AiOiJ1c2VyX2ltcGVyc29uYXRpb24iLCJzaWQiOiIwMDIxNGI4OS00ZTk1LTQ5NWItYTVjZS0yODg3ZThkYjc0YmUiLCJzdWIiOiJQQzVrZFNTUEVsM0hWa3haX3B1cy0wdnQ1dEtZWEZLZVh5OTJaN2lKZFRRIiwidGlkIjoiYTUzYzc0NmYtNzlhNy00ZWQ4LWE2NGItYmJmMTNhM2JkYTZhIiwidW5pcXVlX25hbWUiOiJicm15ZXJzQGJhbXNxbC5vcmciLCJ1cG4iOiJicm15ZXJzQGJhbXNxbC5vcmciLCJ1dGkiOiIzSnRqUy04M1lrV0drTHFhU2ZZZ0FBIiwidmVyIjoiMS4wIiwid2lkcyI6WyI2MmU5MDM5NC02OWY1LTQyMzctOTE5MC0wMTIxNzcxNDVlMTAiLCJiNzlmYmY0ZC0zZWY5LTQ2ODktODE0My03NmIxOTRlODU1MDkiXSwieG1zX2lkcmVsIjoiMSAyMiJ9.KBmZFLpBD_H9_f6bjscuBRIpDQR7It0L-WggIZg9v6Lww0cF6cNL6QK1qKC9eLVvZebWWVRrxewjbGXEJrLAxByDRRrvYaVAZptd8XG26iO775jTOM_BqoECVkA-eN024-qAA4CgOs7Yiad8TzYLECnz7h0RUtBjRCZ6_SNRYt8ua4iXGt1PurTkShhGFQtEDaw4mCQ0-jIHOvmJsZUzADFY_5U3TE1-ZNnLpJDZ-pNIAwKjHSOAyoqGHbobFl9fotlpRMRUZJmuQs3Iwh0Y_-hhX5V8AgXE_bbd6BqNmigxD4tp-5vB5QXbPaz9-0wGwyNAoiT0qHWGGE0JuYv-FQ'}
print(token['access_token'])

# headers = {"Authorization": f"Bearer {token['access_token']}"}
headers = {'Authorization': f"Bearer {token['access_token']}", 'Content-type': 'application/json'}
response = request_call(method='get', url="https://api.fabric.microsoft.com/v1/workspaces", headers=headers)

for workspace in response.json().get('value'):
    # {'id': 'daa15978-fabf-457e-a2ed-ca15c08e9ef8', 'displayName': 'WS_Demo', 'description': '', 'type': 'Workspace', 'capacityId': 'e36151a3-66ab-4bc5-b4f9-8a6e39f79d95'}
    if workspace.get('displayName') == workspaceName:
        workspace_id = workspace.get('id')
        break

os.makedirs(f'FabricWorkspaceFetch/{workspaceName}', exist_ok=True)

response = request_call(method='get', url=f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items", headers=headers)

for item in response.json().get('value'):
    print(item)
    response = request_long_running_call(method='post', url=f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items/{item.get('id')}/getDefinition", headers=headers)
    if response.status_code == 200:
        with open(f"FabricWorkspaceFetch/{workspaceName}/{item.get('displayName')}_{item.get('type')}.json", "w") as f:
            f.write(json.dumps(response.json(), indent=4))

"""
Steps:
    1. List workspaces - https://learn.microsoft.com/en-us/rest/api/fabric/core/workspaces/list-workspaces?tabs=HTTP
    2. Get workspace id from list where workspace name is "WorkspaceName"
    3. List workspace items - https://learn.microsoft.com/en-us/rest/api/fabric/core/items/list-items?tabs=HTTP
    4. For each item, get item definition - https://learn.microsoft.com/en-us/rest/api/fabric/core/items/get-item-definition?tabs=HTTP
    5. Save each item definition to a file - Do we need to extract the "parts" section to separate files? (I think so.)

Notes: 
    Should probably look at using threading to speed up the process since each get item definition is a long running call.
"""
