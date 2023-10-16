from keycloak import KeycloakOpenID
from py4lexis.helper import Clr, _RR, igev, _vreen, ngano


class kck_oi():

    def __init__(self):
        self.Clr = Clr() 
        self._oid = KeycloakOpenID(self.Clr.yhbrr(_RR) + "/auth/", self.Clr.yhbrr(igev), self.Clr.yhbrr(_vreen), client_secret_key=self.Clr.yhbrr(ngano))
               
    def token(self, username, pwd):
        token: dict | dict[str, str] = self._oid.token(username, pwd, scope=["openid"])
        
        return token
    
    def rfsh_token(self, token):
        return self._oid.refresh_token(token)