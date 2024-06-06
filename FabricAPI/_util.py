import tomllib, base64, json, datetime
from azure.identity import InteractiveBrowserCredential
import os


CACHE_FILE = os.path.join(os.path.dirname(os.path.realpath(__file__)), '_cache_token.json')


def _get_token_cached(audience:str="Fabric") -> str:
    token = ''
    # with open('.env.toml', 'rb') as f:
    #     config = tomllib.load(f)
    #     token = config['EnvironmentVariables']['Token_Fabric']
    if _token_cache_file_exists() and _token_cache_audience_exists(audience):
        with open(CACHE_FILE, 'rb') as f:
            token = json.load(f)[audience]
            if _token_cache_expired(token):
                _write_token_to_cache()
                token = _get_token_cached()
    else:
        _write_token_to_cache()
        token = _get_token_cached()

    return token


def _token_cache_audience_exists(audience:str) -> bool:
    with open(CACHE_FILE, 'rb') as f:
        token = json.load(f)
        if audience in token.keys():
            return True
        else:
            print('Token Not Found')
            return False


def _token_cache_expired(token:str) -> bool:
    expiration_date = _get_token_expiration_date(token=token)
    if expiration_date < datetime.datetime.now() + datetime.timedelta(minutes=1):
        return True
    else:
        return False
    

def _decode_token(token) -> dict:
    base64_meta_data = token.split(".")[1].encode("utf-8") + b'=='
    json_bytes = base64.decodebytes(base64_meta_data)
    json_string = json_bytes.decode("utf-8")
    json_dict = json.loads(json_string)
    return json_dict


def _get_token_expiration_date(token, localTZ:bool=True) -> str:
    json_dict = _decode_token(token=token)
    if localTZ:
        expiration_date = datetime.datetime.fromtimestamp(json_dict["exp"])
    else:
        expiration_date = datetime.datetime.fromtimestamp(datetime.UTC, json_dict["exp"])
    return expiration_date


def _token_cache_file_exists():
    if os.path.exists(CACHE_FILE):
        return True
    else:  
        return False


def _get_token_fabric() -> str:
    accessToken = InteractiveBrowserCredential().get_token("https://api.fabric.microsoft.com/.default")
    return accessToken.token


def _write_token_to_cache():
    tokenDict = {"Fabric": _get_token_fabric()}
    with open(CACHE_FILE, 'w') as f:
        json.dump(tokenDict, f)


def _base64_decode_bytes(base64String:str) -> bytes:
    return base64.b64decode(base64String.encode('utf-8'))


def _base64_decode(base64String:str) -> str:
    return _base64_decode_bytes(base64String).decode('utf-8')


def _base64_encode_bytes(string:str) -> bytes:
    return base64.b64encode(string.encode('utf-8'))


def _base64_encode(string:str) -> str:
    return _base64_encode_bytes(string).decode('utf-8')



if __name__ == '__main__':

    # print(_base64_encode('this is a test'))
    # print(_base64_decode('dGhpcyBpcyBhIHRlc3Q='))



    token = _get_token_cached()
    print(_decode_token(token=token))
    print(_get_token_expiration_date(token=token, localTZ=True))
    ## _write_token_to_cache()
    token = _get_token_cached()
    print(_decode_token(token=token))
    print(_get_token_expiration_date(token=token, localTZ=True))


    # myString = 'this is a test'
    # myStringBase64Bytes = base64.b64encode(myString.encode('utf-8'))
    # print(myStringBase64Bytes) #base64 as bytes b''
    # print(myStringBase64Bytes.decode('utf-8')) #base64 in as string ''

    # myStringDecoded = base64.b64decode(myStringBase64Bytes.decode('utf-8'))
    # print(myStringDecoded) #decoded as bytes b''
    # print(myStringDecoded.decode('utf-8')) #decoded as string ''


