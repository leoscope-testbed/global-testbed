import json
from enum import Enum

class LeotestUserRoles(Enum):
    ADMIN = 0
    USER = 1
    NODE = 2
    USER_PRIV = 3
    NODE_PRIV = 4
    NODE_OWNER = 5

class LeotestUser:
    def __init__(self, id, name, role, team, 
                static_access_token='', access_token='', **metadata) -> None:
        self.id = id
        self.name = name 
        self.role = role 
        self.team = team 
        self.static_access_token = static_access_token
        self.access_token = access_token
        self.metadata = metadata
    
    def document(self):
        document = {
            'id': self.id,
            'name': self.name,
            'role': self.role,
            'team': self.team,
            'static_access_token': self.static_access_token,
            'access_token': self.access_token
        }
        document.update({
            key: value
            for key, value in self.metadata.items()
            if value is not None
        })
        return document
    
    def serialize(self):
        return json.dumps(self.document())
