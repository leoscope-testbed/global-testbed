from dateutil.parser import parse as datetimeParse 

class LeotestNode:
    def __init__(self, 
                nodeid, 
                coords, 
                location, 
                last_active='2020-01-01',
                name='', 
                description='Leopard Node', 
                provider='starlink', 
                jobs=None,
                scavenger_mode_active=False,
                public_ip=''):
        """init"""

        self.nodeid = nodeid
        self.name = name 
        self.description = description
        self.last_active = str(last_active)
        self.coords = coords 
        self.location = location
        self.provider = provider 
        self.jobs = jobs  
        self.scavenger_mode_active = scavenger_mode_active
        self.public_ip = public_ip
    
    def document(self):

        node = {
            'nodeid': self.nodeid,
            'name': self.name,
            'description': self.description,
            'last_active': datetimeParse(self.last_active),
            'coords': self.coords,
            'location': self.location,
            'provider': self.provider,
            'jobs': str(self.jobs),
            'scavenger_mode_active': self.scavenger_mode_active,
            'public_ip': self.public_ip
        }

        return node
    
    def document_proto_compatible(self):
        node = self.document()
        node['last_active'] = str(node['last_active'])
        return node