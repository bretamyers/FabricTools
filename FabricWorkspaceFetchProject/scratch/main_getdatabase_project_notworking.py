# from msal import PublicClientApplication
import msal
import json, requests
import os, time

def request_call(method, url, headers) -> requests.Response:
    response = requests.request(method=method, url=url, headers=headers)
    return response

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

cred = msal.PublicClientApplication(client_id=DEVELOPER_SIGN_ON_CLIENT_ID, authority="https://login.microsoftonline.com/organizations")
token = cred.acquire_token_interactive(scopes=["https://api.fabric.microsoft.com/.default"])
# cred = msal.PublicClientApplication(client_id=DEVELOPER_SIGN_ON_CLIENT_ID, authority="https://login.microsoftonline.com/organizations")
# token = cred.acquire_token_interactive(scopes=["https://analysis.windows.net/powerbi/api/.default"])

# token = {'access_token': 'eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsIng1dCI6ImltaTBZMnowZFlLeEJ0dEFxS19UdDVoWUJUayIsImtpZCI6ImltaTBZMnowZFlLeEJ0dEFxS19UdDVoWUJUayJ9.eyJhdWQiOiJodHRwczovL2FwaS5mYWJyaWMubWljcm9zb2Z0LmNvbSIsImlzcyI6Imh0dHBzOi8vc3RzLndpbmRvd3MubmV0L2E1M2M3NDZmLTc5YTctNGVkOC1hNjRiLWJiZjEzYTNiZGE2YS8iLCJpYXQiOjE3NDA3NzA5MjksIm5iZiI6MTc0MDc3MDkyOSwiZXhwIjoxNzQwNzc1NjU3LCJhY2N0IjowLCJhY3IiOiIxIiwiYWlvIjoiQWFRQVcvOFpBQUFBUXc2citYVVlqc01UcUsvbURJdGZIZU0yeTYzTE9ibytjMnJ4TXc2SWdHVDJibk1rdEJuZktWU2dOdnUrckJzajlwNDBDMGlhTEkxTlpZN3ZvWUtWZE1XMUxMUFdFMEZxcTdtVFN0eU9KZzVWVlZUVmlVMk02U1ZBUnJudUpvWWUwWVhrNzBXenFGTnVxWDdkSXhFKy93OGJETjlHTDUyb0N5SnY3amNndzE0aUVHWlRQTlBhUm02c0JYYXdFS0NCTTFhL2pUZ0ZnS3Jpb09uVDNBTEVMZz09IiwiYW1yIjpbInB3ZCIsImZpZG8iLCJtZmEiXSwiYXBwaWQiOiIwNGIwNzc5NS04ZGRiLTQ2MWEtYmJlZS0wMmY5ZTFiZjdiNDYiLCJhcHBpZGFjciI6IjAiLCJmYW1pbHlfbmFtZSI6Ik15ZXJzIiwiZ2l2ZW5fbmFtZSI6IkJyZXQiLCJpZHR5cCI6InVzZXIiLCJpcGFkZHIiOiI0Ny4yMjUuMC4xNjQiLCJuYW1lIjoiQnJldCIsIm9pZCI6ImU0Nzc3MTg5LTQ2ZjktNGYyOC1hZDgwLTA5NjVmMTcwMDQ0NiIsInB1aWQiOiIxMDAzMjAwM0U1NTM0Q0REIiwicmgiOiIxLkFiY0FiM1E4cGFkNTJFNm1TN3Z4T2p2YWFna0FBQUFBQUFBQXdBQUFBQUFBQUFEOEFQNjNBQS4iLCJzY3AiOiJ1c2VyX2ltcGVyc29uYXRpb24iLCJzaWQiOiIwMDIxNGI4OS00ZTk1LTQ5NWItYTVjZS0yODg3ZThkYjc0YmUiLCJzdWIiOiJQQzVrZFNTUEVsM0hWa3haX3B1cy0wdnQ1dEtZWEZLZVh5OTJaN2lKZFRRIiwidGlkIjoiYTUzYzc0NmYtNzlhNy00ZWQ4LWE2NGItYmJmMTNhM2JkYTZhIiwidW5pcXVlX25hbWUiOiJicm15ZXJzQGJhbXNxbC5vcmciLCJ1cG4iOiJicm15ZXJzQGJhbXNxbC5vcmciLCJ1dGkiOiJEWDA2WldaRUlFT2ZPNHpnVGxnY0FBIiwidmVyIjoiMS4wIiwid2lkcyI6WyI2MmU5MDM5NC02OWY1LTQyMzctOTE5MC0wMTIxNzcxNDVlMTAiLCJiNzlmYmY0ZC0zZWY5LTQ2ODktODE0My03NmIxOTRlODU1MDkiXSwieG1zX2lkcmVsIjoiMjIgMSJ9.dPOpL8RDnLYJGKV2xPJGHq96kO2rtcNmF-jpqNb4r3lplKDScHP3Uqi96x2Jfys3kipbbvPTa3uPDIKc0S3I_9u6JO3iNCZ5UpC2p8WWXu2NvXzm4sLxioxa5vv1h2s-Q4nQbBxxZDGsbL6kP-dncf-hpOY9yxmA9iG-L9X-mtdFx3GvpBs-S3TLX_HTUAH_rhNRpss6AC8HNsXv1KOhRQdIRS3TrSKjkF2JoSZhlEisKCzuVG_UzvkjX6RNsmAAdZ_RLloiNh_mUt1r3qJRCa-3d8p_oeFhRsA2Dy1E-znnWIcxIp3XEAtj5PvtEYE7Ed_FPoqrSKjDoqtbfOVDNQ'}
print(token['access_token'])

# headers = {"Authorization": f"Bearer {token['access_token']}"}
headers = {'Authorization': f"Bearer {token['access_token']}", 'Content-type': 'application/json'}
warehouseId = '7d66bdae-f996-4945-b353-dfbe455af67e'
response = request_call(method='get', url="https://api.fabric.microsoft.com/v1/myorg/datawarehouses/{warehouseId}/Export", headers=headers)
# response = request_call(method='get', url="https://api.powerbi.com/v1.0/myorg/lhdatamarts/{warehouseId}/Export", headers=headers)
# response = request_call(method='get', url="https://wabi-west-us3-a-primary-redirect.analysis.windows.net/v1.0/myorg/datawarehouses/7d66bdae-f996-4945-b353-dfbe455af67e/export", headers=headers)
print(response)
