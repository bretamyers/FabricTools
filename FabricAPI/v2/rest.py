
import requests
import datetime
import math
import logging
import time
import json

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

class rest:
    def __init__(self, audience:str='pbi'):
        self.header = self.create_header(audience)

    def create_header(self, audience:str='pbi') -> dict:
        import _util
        token = _util._get_token_cached()
        return {'Authorization': f'Bearer {token}', 'Content-type': 'application/json'}
    
    def request(self, method:str, url:str, body:dict=None) -> requests.Response:
        try:
            response = requests.request(method=method, url=url, headers=self.header, data=json.dumps(body))
            logger.debug(response.json())
            response.raise_for_status()
            logger.debug(f"Response - {response.status_code}")
            if response.status_code == 202:
                response = self._response_long_running(response=response)
            return response
        except requests.exceptions.HTTPError as errh:
            print("Http Error:", errh.response.text)
            ## Add a step to check if the error is due to throttling and wait until the restriction is lifted
            if 'Request is blocked by the upstream service until:' in errh.response.json()['message']:
                blockedDatetime = datetime.datetime.strptime(errh.response.json()['message'].split('Request is blocked by the upstream service until: ')[1], '%m/%d/%Y %I:%M:%S %p')
                sleepDuration = math.ceil((blockedDatetime - datetime.datetime.now(datetime.UTC).replace(tzinfo=None)).total_seconds())
                logger.info(f"Sleeping for {sleepDuration} seconds")
                time.sleep(sleepDuration) # pause until we can make the request again
                return self.request(method=method, url=url, body=body)
        except requests.exceptions.ConnectionError as errc:
            print("Error Connecting:", errc.response.text)
        except requests.exceptions.Timeout as errt:
            print("Timeout Error:", errt.response.text)
        except requests.exceptions.RequestException as err:
            print(response.status_code)

    def _response_long_running(self, response:requests.Response) -> requests.Response:
        responseLocation = response.headers.get('Location')
        # Will pause 5 unique times before failing
        for _ in range(5):
            responseStatus = self.request(method='get', url=responseLocation)
            if responseStatus.json().get('status') != 'Succeeded':
                logger.info(f'Operation {response.headers.get("x-ms-operation-id")} is not ready. Waiting for {response.headers.get("Retry-After")} seconds.')
                time.sleep(int(response.headers.get('Retry-After')))
            else:
                logger.info('Payload is ready. Requesting the result.')
                responseResult = self.request(method='get', url=f'{responseLocation}/result')
                return responseResult
            

