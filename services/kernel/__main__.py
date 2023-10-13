import os
import sys 
import socket
import docker 
import argparse
import logging
import subprocess
from common.client import LeotestClient
from google.protobuf.json_format import MessageToDict

logging.basicConfig(
    level=logging.INFO, 
    format="%(asctime)s %(filename)s:%(lineno)s %(thread)d %(levelname)s %(message)s")

log = logging.getLogger(__name__)

def get_userid_from_ipaddr(ip_addr, network='global-testbed_leotest-net'):
    userid = None
    client = docker.from_env()
    # get the list of containers in the network
    leotest_net = client.networks.get(network)
    containers = leotest_net.containers

    for container in containers:
        name_c = container.name 
        ip_addr_c = container.attrs['NetworkSettings']["Networks"][network]["IPAddress"] 
        log.info('candidate: container_name=%s container_ip=%s source_ip=%s'
                    % (name_c, ip_addr_c, ip_addr)
        )
        if ip_addr_c == ip_addr:
            # get labels from the container 
            labels = container.labels
            if "userid" in labels:
                userid = labels["userid"]
            
            break
    return userid

def execute_command(command):
    # /bin/sh -c 'echo 10 > /sys/module/tcp_bbr2/parameters/loss_thresh'
    output = subprocess.Popen(command.split(), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    # output.stdin.write('tcpdump started'.encode())
    output = b"Command executed."
    return output

def check_cca():
    cmd = "sysctl net.ipv4.tcp_congestion_control"
    output = subprocess.check_output(cmd.split(), stdin=subprocess.PIPE, stderr=subprocess.PIPE)
    return output

def change_cca(cca):
    cmd = f"sysctl -w net.ipv4.tcp_congestion_control={cca}"
    output = subprocess.check_output(cmd.split(), stdin=subprocess.PIPE, stderr=subprocess.PIPE)
    return output

def set_bbr2_param(path, value):
    with open(path, 'w') as f:
        f.write(value)
    
    return b'BBR2 parameter %s set to %s' % (path.encode(), value.encode())

def main():
    
    parser = argparse.ArgumentParser(description='Leotest kernel service.')
    parser.add_argument("--nodeid", type=str, required=True, 
											help='nodeid')
    parser.add_argument("--grpc-hostname", type=str, required=False, 
											help='gRPC hostname', default="localhost")
    parser.add_argument("--grpc-port", type=int, required=False, 
											help='gRPC port', default=50051)   

    parser.add_argument("--access-token", type=str, required=False, 
											help='node access token', 
                                            default="leotest-access-token")   
    parser.add_argument("--leotest-net", type=str, required=False, 
											help='docker network for leotest', 
                                            default="global-testbed_leotest-net")                                                             
    args = parser.parse_args()

    nodeid = args.nodeid
    grpc_hostname = args.grpc_hostname
    grpc_port = args.grpc_port
    access_token = args.access_token

    leotest_client = LeotestClient(grpc_hostname=grpc_hostname, 
							        grpc_port=grpc_port,
							        userid=nodeid, 
                                    access_token=access_token)

    print('Opening socket..')

    # create a socket object
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    # get local machine name
    # host = socket.gethostname()
    # host = "10.0.3.2"
    # host = server_mappings[location][0][1]
    host = "0.0.0.0"
    port = 9000

    # bind the socket to a public host, and a well-known port
    s.bind((host, port))

    # eth = "veth6"
    # become a server socket
    log.info('Setting up server...')
    s.listen(1)

    log.info("Server started on {}:{}".format(host, port))

    while True:
        log.info('Waiting for connections')
        # accept connections from outside
        (clientsocket, address) = s.accept()

        log.info("Connection from {}".format(address))

        ip_addr = address[0]
        userid = get_userid_from_ipaddr(ip_addr, network=args.leotest_net)
        if userid:
            res = leotest_client.kernel_access(userid=userid)
            res = MessageToDict(res)
            log.info(res)
        else:
            res = {'state': 'FAILED'}
        # verify access for userid 
        if 'state' in res and res['state']=='FAILED':
            log.info('insufficient access level: access denied')
            clientsocket.send(b'access_denied')
            clientsocket.close()
            continue 

        # receive command from client
        try:
            command = clientsocket.recv(1024).decode()
        except Exception as e:
            command = "cca.check"

        log.info("Received command: {}".format(command))

        # execute command and send output back to client
        # output = execute_command(command)
        try:
            if command == "cca.check":
                output = check_cca()
            elif command.startswith("cca.change"):
                output = change_cca(command.split()[1])     # cmd format "cca.change cubic"
            elif command.startswith("bbr2.param.set"):
                output = set_bbr2_param(command.split()[1], command.split()[2])
            else:
                output = execute_command(command)
            clientsocket.send(output)

            # close the connection
            clientsocket.close()

        except Exception as e:
            print(e)
    

if __name__ == "__main__":
    main()


