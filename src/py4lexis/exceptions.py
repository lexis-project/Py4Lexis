from keycloak.exceptions import KeycloakPostError, KeycloakAuthenticationError

class Py4LexisAuthException(KeycloakAuthenticationError):
    pass

class Py4LexisPostException(KeycloakPostError):
    pass

class Py4LexisException(Exception):
    pass