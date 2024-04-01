
import _rest
import fabric_workspace, fabric_capacity

class fabric(_rest.rest_call, fabric_workspace.workspace, fabric_capacity.capacity):
    
    def __init__(self):
        self.a = None

    # class workspace(fabric_workspace.workspace):
    #     def __init__(self):
    #         self.child = 2
        
    #     def get_id(self):
    #         return self.child

    # class capacity(test.rest_base):


if __name__ == '__main__':
    
    myObj = fabric()
    # print(myObj.get_id())
    
    # myObj = child()
    # print(myObj.get_id())
    # print(myObj)





