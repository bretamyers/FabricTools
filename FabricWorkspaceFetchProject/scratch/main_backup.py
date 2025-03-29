# from msal import PublicClientApplication
import msal
import json, requests
import os

def request_call(method, url, headers) -> requests.Response:
    response = requests.request(method=method, url=url, headers=headers)
    return response

def request_long_running_call(method, url, headers) -> requests.Response:
    response = request_call(method=method, url=url, headers=headers)

    # If accepted
    if response.status_code == 202:
        _completed = False
        _retry_after = response.headers["Retry-After"] if "Retry-After" in response.headers else None
        _result = {}
        for _ in range(retry_attempts):
            time.sleep(_retry_after)
            op_status = request_call(
                method='get',
                url=response.headers["Location"] if "Location" in response.headers else None,
                headers=headers
            )
            # If it's a non-terminal state, keep going
            if op_status.body["status"] in ["NotStarted", "Running", "Undefined"]:
                _retry_after = int(op_status.headers["Retry-After"] if "Retry-After" in response.headers else None)
                continue
            # Successful state, get the response from the Location header
            elif op_status.json()["status"] == "Succeeded":
                _completed = True
                # If the operation is complete, but there is no location provided, then return the last requested payload.
                if op_status.headers["Location"] if "Location" in response.headers else None is None:
                    _result = op_status
                else:
                    op_results = request_call(
                        method='get',
                        url=op_status.headers["Location"] if "Location" in response.headers else None,
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
            raise Exception("Operation did not complete in time.")


DEVELOPER_SIGN_ON_CLIENT_ID = "04b07795-8ddb-461a-bbee-02f9e1bf7b46"

# cred = msal.PublicClientApplication(client_id=DEVELOPER_SIGN_ON_CLIENT_ID, authority="https://login.microsoftonline.com/organizations")
# token = cred.acquire_token_interactive(scopes=["https://api.fabric.microsoft.com/.default"])

token = {'access_token': 'eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsIng1dCI6ImltaTBZMnowZFlLeEJ0dEFxS19UdDVoWUJUayIsImtpZCI6ImltaTBZMnowZFlLeEJ0dEFxS19UdDVoWUJUayJ9.eyJhdWQiOiJodHRwczovL2FwaS5mYWJyaWMubWljcm9zb2Z0LmNvbSIsImlzcyI6Imh0dHBzOi8vc3RzLndpbmRvd3MubmV0L2E1M2M3NDZmLTc5YTctNGVkOC1hNjRiLWJiZjEzYTNiZGE2YS8iLCJpYXQiOjE3NDA3NjYxNTMsIm5iZiI6MTc0MDc2NjE1MywiZXhwIjoxNzQwNzcxMTEzLCJhY2N0IjowLCJhY3IiOiIxIiwiYWlvIjoiQWFRQVcvOFpBQUFBb1ZqVkFudHdFZC9XRmdxOFVRRDZwQzFZZ2R2UHRsVWVQd3FYNVZFNTY0ejcvdWNxZGdyRDNWbEErbHRuM29Ubzg2NWpOb0dRV29kSVBPNk02OGY0UGthaE9tdnJXSURFYmhBdzhITmdSOW9NL1YvV1hoVFR2MG12V2xRbjFyREF5dndwVGJpN3dsQ2hCYnprRFdvUDYzdDhHZTNTR2JMTmR4cDVmWGNkYnZZRkpWRVRrbE1OMHA5OWg5MlB2enhUSTEvVGhLeG94OFZvQzVXOGlVR0szUT09IiwiYW1yIjpbInB3ZCIsImZpZG8iLCJtZmEiXSwiYXBwaWQiOiIwNGIwNzc5NS04ZGRiLTQ2MWEtYmJlZS0wMmY5ZTFiZjdiNDYiLCJhcHBpZGFjciI6IjAiLCJmYW1pbHlfbmFtZSI6Ik15ZXJzIiwiZ2l2ZW5fbmFtZSI6IkJyZXQiLCJpZHR5cCI6InVzZXIiLCJpcGFkZHIiOiI0Ny4yMjUuMC4xNjQiLCJuYW1lIjoiQnJldCIsIm9pZCI6ImU0Nzc3MTg5LTQ2ZjktNGYyOC1hZDgwLTA5NjVmMTcwMDQ0NiIsInB1aWQiOiIxMDAzMjAwM0U1NTM0Q0REIiwicmgiOiIxLkFiY0FiM1E4cGFkNTJFNm1TN3Z4T2p2YWFna0FBQUFBQUFBQXdBQUFBQUFBQUFEOEFQNjNBQS4iLCJzY3AiOiJ1c2VyX2ltcGVyc29uYXRpb24iLCJzaWQiOiIwMDIxNGI4OS00ZTk1LTQ5NWItYTVjZS0yODg3ZThkYjc0YmUiLCJzdWIiOiJQQzVrZFNTUEVsM0hWa3haX3B1cy0wdnQ1dEtZWEZLZVh5OTJaN2lKZFRRIiwidGlkIjoiYTUzYzc0NmYtNzlhNy00ZWQ4LWE2NGItYmJmMTNhM2JkYTZhIiwidW5pcXVlX25hbWUiOiJicm15ZXJzQGJhbXNxbC5vcmciLCJ1cG4iOiJicm15ZXJzQGJhbXNxbC5vcmciLCJ1dGkiOiJiVkZWbTZwTzJVR3NabG5uQmc4WkFBIiwidmVyIjoiMS4wIiwid2lkcyI6WyI2MmU5MDM5NC02OWY1LTQyMzctOTE5MC0wMTIxNzcxNDVlMTAiLCJiNzlmYmY0ZC0zZWY5LTQ2ODktODE0My03NmIxOTRlODU1MDkiXSwieG1zX2lkcmVsIjoiMSA4In0.m-LpeGKjl72V9QCBvZArJPZTHQzhYP2LoI9RRZlTqQO7T0kgO2-1g8HBzBD5xVO0usCZ8S1vktBLihbjsNefIoc1qxTG4Mems3RUHHmZegpU9-f4eFmMD4vtwCp1-LcyPcYzzRBpOUJc3GVrSRZVc9ySOP5L1n6HItMpVy524nEPz4wO7mncEPFFWfEWFtL0k1SgV6fHRP8P3awlaiHKdFAXjBAq5yluoI3JfD6e3I4SbBPTdl00ijgtpErevwZM40FJ-_obhvvpWVLGwxnShsl7-xozO8kJ7Lx2OCScN2Y9f-Ff9956-pxxPgkbV8MGheBWYya0bvP4i3SuM_eARw'}
print(token['access_token'])

# headers = {"Authorization": f"Bearer {token['access_token']}"}
headers = {'Authorization': f"Bearer {token['access_token']}", 'Content-type': 'application/json'}
response = requests.request(method='get', url="https://api.fabric.microsoft.com/v1/workspaces", headers=headers)

for workspace in response.json().get('value'):
    # {'id': 'daa15978-fabf-457e-a2ed-ca15c08e9ef8', 'displayName': 'WS_Demo', 'description': '', 'type': 'Workspace', 'capacityId': 'e36151a3-66ab-4bc5-b4f9-8a6e39f79d95'}
    if workspace.get('displayName') == "WS_Demo":
        workspace_id = workspace.get('id')
        break

os.makedirs('WS_Demo', exist_ok=True)

response = requests.request(method='get', url=f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items", headers=headers)

for item in response.json().get('value'):
    print(item)
    response = requests.request(method='post', url=f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items/{item.get('id')}/getDefinition", headers=headers)
    with open(f"WS_Demo/{item.get('displayName')}.json", "w") as f:
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
