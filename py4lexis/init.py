from py4lexis.lexis_session import LexisSession
from configparser import ConfigParser
from os.path import exists


def init_api_session():
    """
        Method initialise LEXIS api session using config file "config.toml".

        Return
        -------
        Created LEXIS api session.
    """
    config_exists = exists('config.toml')
    if config_exists:
        try:
            config = ConfigParser()
            config.read('config.toml')

            USERNAME = config.get('config', 'USERNAME')
            PWD = config.get('config', 'PWD')
            KEYCLOAK_URL = config.get('config', 'KEYCLOAK_URL')
            REALM = config.get('config', 'REALM')
            CLIENT_ID = config.get('config', 'CLIENT_ID')
            CLIENT_SECRET = config.get('config', 'CLIENT_SECRET')
            DDI_ENDPOINT_URL = config.get('config', 'DDI_ENDPOINT_URL')
            ZONENAME = config.get('config', 'ZONENAME')

            api_session = LexisSession(USERNAME, PWD, KEYCLOAK_URL, REALM, CLIENT_ID, CLIENT_SECRET,
                                       DDI_ENDPOINT_URL, ZONENAME)
        except:
            raise ValueError('Config file has wrong format or is corrupted!')
    else:
        raise ValueError('Config file "config.toml" is not existing within project!')

    return api_session

