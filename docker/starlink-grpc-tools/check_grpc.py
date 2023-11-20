import sys 
import redis 
import socket
from subprocess import call, check_output

# python3 dish_grpc_mqtt.py --port 1883 -t 2 status
# python check_grpc.py dish_grpc_mqtt.py --port 1883 -t 2 status 
# 192.168.100.1:9200

sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock.settimeout(1)
result = sock.connect_ex(('192.168.100.1', 9200))
r = redis.Redis(host='redis', port='6379', db=0)
if result == 0:
    print('Dish API detected. Launching status dump scripts.')
    
    # get the terminal id 
    opts = ["python", "dish_grpc_text.py", "status"]
    out = check_output(opts)
    print(out)
    utid = out.decode('utf-8').split(',')[1]

    print('Starlink terminal utid=%s' % utid)
    print('Setting terminal id on redis cache; key="starlink-terminal-id" ')
    # put the utid onto the redis store
    
    r.set('starlink-terminal-id', utid)
    r.set('starlink-terminal-found', 'True')
    r.close()
    
    # launch the script with the arguments 
    opts = ["python"]
    opts.extend(sys.argv[1:])
    call(opts)
else:
    r.set('starlink-terminal-found', 'False')
    r.close()
    print('No Dish API found. Exiting.')
    # exit 
    exit(0)
sock.close()