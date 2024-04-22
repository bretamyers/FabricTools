
import _restclient

class semantic_model():
    def __init__(self, header:str=None):
        if header is None:
            self.header = _restclient.rest().header
        else:    
            self.header = header

    def get_name(self):
        return 'name'
    