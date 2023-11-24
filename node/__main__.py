"""Entry point for the testbed node."""

import sys 
import argparse
import logging
from subprocess import call
from node.scheduler import scheduler_loop


logging.basicConfig(
    level=logging.INFO, 
    format="%(asctime)s %(filename)s:%(lineno)s %(thread)d %(levelname)s %(message)s")

log = logging.getLogger(__name__)

def main():
    
    parser = argparse.ArgumentParser(description='Leotest node.')
    parser.add_argument("--nodeid", type=str, required=True, 
                                            help='nodeid')
    parser.add_argument("--grpc-hostname", type=str, required=False, 
                                            help='gRPC hostname', default="localhost")
    parser.add_argument("--grpc-port", type=int, required=False, 
                                            help='gRPC port', default=50051)
    parser.add_argument("--interval", type=int, required=False, 
                                            help='orchestrator polling interval', 
                                            default=10)

    parser.add_argument("--executor-config", type=str, required=False, 
                                            help='executor config yaml', 
                                            default="/executor-config.yaml")  

    parser.add_argument("--workdir", type=str, required=False, 
                                            help='working directory', 
                                            default="/home/leotest/")   
    parser.add_argument("--artifactdir", type=str, required=False, 
                                            help='artifacts directory', 
                                            default="/artifacts/")      

    parser.add_argument("--access-token", type=str, required=False, 
                                            help='node access token', 
                                            default="leotest-access-token")                                                                   
    args = parser.parse_args()

    nodeid = args.nodeid
    grpc_hostname = args.grpc_hostname
    grpc_port = args.grpc_port
    interval = args.interval
    workdir = args.workdir
    artifactdir = args.artifactdir
    executor_config = args.executor_config
    access_token = args.access_token

    # cron service doesn't start automatically on docker containers
    log.info('restarting cron service')
    call(['service', 'cron', 'restart'])
    log.info('restarting atd service')
    call(['service', 'atd', 'restart'])
    try:
        log.info('starting scheduler loop')
        scheduler_loop(nodeid, 
                    grpc_hostname=grpc_hostname, 
                    grpc_port=grpc_port, 
                    interval=interval,
                    workdir=workdir,
                    artifactdir=artifactdir,
                    executor_config=executor_config,
                    access_token=access_token)

    except (KeyboardInterrupt, SystemExit):
        log.warning("received interrupt in main")  
        sys.exit(0)
    

if __name__ == "__main__":
    main()


