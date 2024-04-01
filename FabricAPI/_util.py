


def _get_token_cached():
    import tomllib

    token = ''
    with open('FabricAPI/.env.toml', 'rb') as f:
        config = tomllib.load(f)
        token = config['EnvironmentVariables']['Token_Fabric']
        
    return token


