from keycloak import KeycloakOpenID, KeycloakPostError
from keycloak.exceptions import KeycloakAuthenticationError
from irods.session import iRODSSession
from py4lexis.exceptions import Py4LexisAuthException, Py4LexisPostException
from py4lexis.helper import Clr, _RR, igev, _vreen, ngano, _urby, _gomiz, _ulme, _itbbra, _yuo


class kck_oi():

    def __init__(self):
        self.Clr = Clr() 
        self._oid = KeycloakOpenID(self.Clr.yhbrr(_RR) + "/auth/", self.Clr.yhbrr(igev), self.Clr.yhbrr(_vreen), client_secret_key=self.Clr.yhbrr(ngano))
        
               
    def token(self, username, pwd):
        try:
            token: dict | dict[str, str] = self._oid.token(username, pwd, scope=["openid"])
        
            return token
        
        except KeycloakAuthenticationError:
            raise Py4LexisAuthException
        
        except KeycloakPostError:
            raise Py4LexisPostException
        
    
    def rfsh_token(self, token):
        try:
            return self._oid.refresh_token(token)
        
        except KeycloakPostError:
            raise Py4LexisPostException
    
    
    def get_irods_session(self, username, token):
        return iRODSSession(
            host=self.Clr.yhbrr(_urby),
            port=int(self.Clr.yhbrr(_gomiz)),
            authentication_scheme=self.Clr.yhbrr(_ulme),
            openid_provider=self.Clr.yhbrr(_itbbra),
            zone=self.Clr.yhbrr(_yuo),
            access_token=token,
            user=username,
            block_on_authURL=False)