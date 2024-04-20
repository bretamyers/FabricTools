import requests


class restclient():
    def __init__(self):
        self.session = requests.Session()
        self.headers = {
            'Content-Type': 'application/json',
        }

    def request(self, url, method='GET', data=None):
        response = self.session.request(method, url, headers=self.headers, data=data)
        return response.text
    
