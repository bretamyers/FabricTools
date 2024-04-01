
import fabric_base

class capacity():

    def __init__(self):
        self.parent = 1
    
    def capacity_get_id(self):
        return 'capacity'


# class fabric(rest_base):

#     class workspace(rest_base):
#         def __init__(self):
#             self.child = 2
        
#         def get_id(self):
#             return self.child

    
if __name__ == '__main__':
    
    myObj = fabric_base.fabric()
    print(myObj.get_id())
    
    # myObj = child()
    # print(myObj.get_id())
    # print(myObj)





