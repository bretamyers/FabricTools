import tomllib, base64


def _get_token_cached():
    token = ''
    with open('.env.toml', 'rb') as f:
        config = tomllib.load(f)
        token = config['EnvironmentVariables']['Token_Fabric']
        
    return token

def _base64_decode_bytes(base64String:str) -> str:
    return base64.b64decode(base64String.encode('utf-8'))

def _base64_decode(base64String:str) -> str:
    return _base64_decode_bytes(base64String).decode('utf-8')

def _base64_encode_bytes(string:str) -> str:
    return base64.b64encode(string.encode('utf-8'))

def _base64_encode(string:str) -> str:
    return _base64_encode_bytes(string).decode('utf-8')



if __name__ == '__main__':

    print(_base64_encode('this is a test'))
    print(_base64_decode('dGhpcyBpcyBhIHRlc3Q='))


    # myString = 'this is a test'
    # myStringBase64Bytes = base64.b64encode(myString.encode('utf-8'))
    # print(myStringBase64Bytes) #base64 as bytes b''
    # print(myStringBase64Bytes.decode('utf-8')) #base64 in as string ''

    # myStringDecoded = base64.b64decode(myStringBase64Bytes.decode('utf-8'))
    # print(myStringDecoded) #decoded as bytes b''
    # print(myStringDecoded.decode('utf-8')) #decoded as string ''


