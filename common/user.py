import json
from enum import Enum

class LeotestUserRoles(Enum):
    ADMIN = 0
    USER = 1
    NODE = 2
    USER_PRIV = 3
    NODE_PRIV = 4

class LeotestUser:
    def __init__(self, id, name, role, team, 
                static_access_token='', access_token='') -> None:
        self.id = id
        self.name = name 
        self.role = role 
        self.team = team 
        self.static_access_token = static_access_token
        self.access_token = access_token
    
    def document(self):
        return {
            'id': self.id,
            'name': self.name,
            'role': self.role,
            'team': self.team,
            'static_access_token': self.static_access_token,
            'access_token': self.access_token
        }
    
    def serialize(self):
        return json.dumps(self.document())