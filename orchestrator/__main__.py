import sys 
import argparse 
import logging
from orchestrator.orchestrator import LeotestOrchestrator

logging.basicConfig(
    level=logging.INFO, 
    format="%(asctime)s %(filename)s:%(lineno)s %(thread)d %(levelname)s %(message)s")

log = logging.getLogger(__name__)

def main():
    parser = argparse.ArgumentParser(description='Leotest orchestrator.')
    parser.add_argument("--grpc-hostname", type=str, required=False, 
                                            help='hostname', default='localhost')
    parser.add_argument("--grpc-port", type=int, required=False, 
                                            help='port', default=50051)
    parser.add_argument("--grpc-workers", type=int, required=False, 
                                            help='workers', default=10)

    parser.add_argument("--db-server", type=str, required=False, 
                                help='database server', default='localhost')

    parser.add_argument("--db-port", type=int, required=False, 
                                help='database port', default=27017)

    parser.add_argument("--db-name", type=str, required=False, 
                                help='database name', default='leotest')
    
    parser.add_argument("--admin-access-token", type=str, required=False, 
                                help='access token for admin', 
                                default='leotest-access-token')

    parser.add_argument("--jwt-secret", type=str, required=False, 
                                help='JWT shared secret for verifying JWT tokens', 
                                default='jwt-secret')

    parser.add_argument("--jwt-algo", type=str, required=False, 
                                help='JWT shared algorithm for verifying JWT tokens', 
                                default='jwt-algo')
    args = parser.parse_args()

    params = {
        'grpc_hostname': args.grpc_hostname, 
        'grpc_port': args.grpc_port,
        'grpc_max_workers': args.grpc_workers,
        'db_server': args.db_server,
        'db_port': args.db_port,
        'db_name': args.db_name,
        'admin_access_token': args.admin_access_token,
        'jwt_secret': args.jwt_secret,
        'jwt_algo': args.jwt_algo 
    }

    try:
        orch = LeotestOrchestrator(**params)
        orch.start()

    except (KeyboardInterrupt, SystemExit):
        log.warning("received interrupt in main")
        sys.exit(0)
    

if __name__ == "__main__":
    main()


