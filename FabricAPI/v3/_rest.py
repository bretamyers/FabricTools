import logging
import requests
import datetime
import time
import math
import json
from typing import List, Iterator


logger = logging.getLogger(__name__)


class FabricRestResponse():
    def __init__(self, method:str='', url:str='', body:dict={}, parser:str='value', parameters:dict={}) -> None:
        self.header = self.create_header(audience='pbi')
        self.responseList = self.request(method=method, url=url, body=body)
        self.parser = parser
        self.parameters = self._response_build_parameters(parameters=parameters)
        


    def __str__(self) -> str:
        return json.dumps(self._response_list_unravel(self.responseList), indent=2)


    def __iter__(self) -> Iterator[int]:
        for response in self._response_list_unravel(self.responseList):
            yield response
    

    def create_header(self, audience:str='pbi') -> dict:
        import _util
        token = _util._get_token_cached()
        return {'Authorization': f'Bearer {token}', 'Content-type': 'application/json'}
    

    def request(self, method:str, url:str, body:dict=None) -> List[requests.Response]:
        logger.info(f'request - {method} - {url} - {body}')
        responseList = []
        def make_request(method:str, url:str, body:dict=None):
            try:
                response = requests.request(method=method, url=url, headers=self.header, data=json.dumps(body))
                logger.debug(response.json())
                response.raise_for_status()
                logger.debug(f"Response - {response.status_code}")
                if response.status_code == 202:
                    response = self.response_long_running(response=response)
                
                responseList.append(response)
                # logger.info(f'make_request - continuationUri: {response.json().get("continuationUri")}')
                if response.json().get('continuationUri') is not None and response.json().get('continuationToken') is not None:
                    logger.info(f'ContinuationUri - {response.json().get("continuationUri")}')
                    # response = self.request(method='get', url=f'{response.json().get("continuationUri")}', responseList=responseList)
                    make_request(method='get', url=f'{response.json().get("continuationUri")}')
            except requests.exceptions.HTTPError as errh:
                ## Add a step to check if the error is due to throttling and wait until the restriction is lifted
                # print(f'{errh.response.json()["message"]=}')
                if 'Request is blocked by the upstream service until:' in errh.response.json()['message']:
                    blockedDatetime = datetime.datetime.strptime(errh.response.json()['message'].split('Request is blocked by the upstream service until: ')[1], '%m/%d/%Y %I:%M:%S %p')
                    sleepDuration = math.ceil((blockedDatetime - datetime.datetime.now(datetime.UTC).replace(tzinfo=None)).total_seconds())
                    logger.info(f"Sleeping for {sleepDuration} seconds")
                    time.sleep(sleepDuration) # pause until we can make the request again
                    #return self.request(method=method, url=url, body=body) # need to look into this. This might be the casuing the error
                    make_request(method=method, url=url, body=body)
                else:
                    raise Exception("Http Error:", errh.response.text)
            except requests.exceptions.ConnectionError as errc:
                raise Exception("Error Connecting:", errc.response.headers)
            except requests.exceptions.Timeout as errt:
                raise Exception("Timeout Error:", errt.response.text)
            except requests.exceptions.RequestException as err:
                raise Exception(response.status_code)
                
        make_request(method=method, url=url, body=body)
        logger.info(f'responseList - {responseList}')
        return responseList


    def response_long_running(self, response:requests.Response) -> requests.Response:
        responseLocation = response.headers.get('Location')
        # Will pause 5 unique times before failing
        for _ in range(5):
            # locationResponse = self.request(method='get', url=responseLocation)
            # logger.info(f'{locationResponse}, {len(locationResponse)}')
            responseStatus = self.request(method='get', url=responseLocation)[0] # Just get the first item in the list because it should only have one item.
            if responseStatus.json().get('status') != 'Succeeded':
                logger.info(f'Operation {response.headers.get("x-ms-operation-id")} is not ready. Waiting for {response.headers.get("Retry-After")} seconds.')
                time.sleep(int(response.headers.get('Retry-After')))
            else:
                if responseStatus.headers.get('Location') is None:
                    logger.info('Long running operation has completed. No result to be returned')
                    return responseStatus
                else:
                    logger.info('Payload is ready. Requesting the result.')
                    # responseResult = self.request(method='get', url=f'{responseLocation}/result')
                    responseResult = self.request(method='get', url=responseStatus.headers.get('Location'))[0] # Just get the first item in the list because it should only have one item.
                    return responseResult
                

    def _response_parse(self, response:List[requests.Response]) -> dict:
        responseParsed = [response.json() for response in response]
        return responseParsed
    

    def _response_list_unravel(self, responseList:List[requests.Response], param:str='value') -> list:
        responseUnraveled = []
        for responseItem in self._response_parse(responseList):
            if responseItem.get(param) is not None:
                for response in responseItem.get(param):
                    responseUnraveled.append(response)
            else:
                responseUnraveled.append(responseItem)
        return responseUnraveled
    

    def _response_build_parameters(self, **paramaters:dict) -> str:
        parameterString = '&'.join([f'{k}={v}' for k,v in paramaters.items() if v is not None])
        responseParameterString = f'?{parameterString}' if parameterString != '' else ''
        return responseParameterString


