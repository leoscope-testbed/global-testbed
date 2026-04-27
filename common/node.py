import json

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
                public_ip='',
                owner='',
                scheduling_enabled=True,
                registered_at='2020-01-01',
                last_status_change='2020-01-01',
                bandwidth_limits_json='[]',
                availability_history_json='[]'):
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
        self.owner = owner
        self.scheduling_enabled = scheduling_enabled
        self.registered_at = str(registered_at or last_active)
        self.last_status_change = str(last_status_change or last_active)
        self.bandwidth_limits_json = self._ensure_json_text(
            bandwidth_limits_json)
        self.availability_history_json = self._ensure_json_text(
            availability_history_json)

    def _ensure_json_text(self, value):
        if value in (None, ''):
            return '[]'
        if isinstance(value, str):
            try:
                json.loads(value)
                return value
            except Exception:
                return '[]'
        try:
            return json.dumps(value)
        except Exception:
            return '[]'
    
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
            'public_ip': self.public_ip,
            'owner': self.owner,
            'scheduling_enabled': self.scheduling_enabled,
            'registered_at': datetimeParse(self.registered_at),
            'last_status_change': datetimeParse(self.last_status_change),
            'bandwidth_limits_json': self.bandwidth_limits_json,
            'availability_history_json': self.availability_history_json
        }

        return node
    
    def document_proto_compatible(self):
        node = self.document()
        node['last_active'] = str(node['last_active'])
        node['registered_at'] = str(node['registered_at'])
        node['last_status_change'] = str(node['last_status_change'])
        return node
