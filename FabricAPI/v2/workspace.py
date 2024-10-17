import _restclient
import item

class workspace():
    def __init__(self, header:str=None):
        if header is None:
            self.header = _restclient.rest().header
        else:    
            self.header = header

        self.item = item.item(self.header_fabric)

    def get_name(self):
        return 'name'

    
    