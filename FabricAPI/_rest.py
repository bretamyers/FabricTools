import requests
import logging
import json

class rest_call():
    def __init__(self):
        self.a = 1

    def request(self):
        requests.request(method='get', url='')

    def header(self) -> dict:
        return {'token': f'Bearer'}
    

