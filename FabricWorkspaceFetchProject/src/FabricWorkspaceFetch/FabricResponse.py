import requests
import json
import time


class FabricException(Exception):
    def __init__(self, message):
        self.message = message
        super().__init__(self.message)


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

