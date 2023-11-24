import yaml
import argparse
from common.client import LeotestClient
from ext_depen.ext_dependency_test import test_ext_dependency


def main():
    parser = argparse.ArgumentParser(description='Leotest command line interface.')

    parser.add_argument("--grpc-host", type=str, required=False, 
                                            default='20.168.9.14',
                                            help='gRPC host')

    parser.add_argument("--grpc-port", type=str, required=False, 
                                            default='50051',
                                            help='gRPC port')

    parser.add_argument("--userid", type=str, required=False, 
                                            default='admin',
                                            help='leotest userid')
    
    parser.add_argument("--access-token", type=str, required=False, 
                                            default='',
                                            help='leotest access token to authenticate userid')

    parser.add_argument("--jwt-access-token", type=str, required=False, 
                                            default='',
                                            help='jwt access token to authenticate userid')

    servicesubparser = parser.add_subparsers(help='config/user/job',
                                        dest="service")

    # actionsubparser = parser.add_subparsers(help='Action to peform: register',
    #                                     dest="action")

    config_parser = servicesubparser.add_parser("config")
    config_parser.add_argument("--action", type=str, required=True,
                                            choices=["update", "get"], 
                                            help='action')
    
    config_parser.add_argument("--path", type=str, required=False,
                                                help='path to config json')

    """
    Parser for user registeration
    Examples: 
    python3 cli.py user --action=register --id=test-id --name=test-user --role=admin --team=leopard-msri
    python3 cli.py user --action=get --id=test-id --name=test-user --role=admin --team=leopard-msri
    """
    user_parser = servicesubparser.add_parser("user")
    user_parser.add_argument("--action", type=str, required=True, 
                                            choices=["register", "get", "modify", "delete"],
                                            help='action to perform on user')

    user_parser.add_argument("--id", type=str, required=True, 
                                            help='userid')
    user_parser.add_argument("--name", type=str, required=False, 
                                            help='name of user')
    user_parser.add_argument("--role", type=str, required=False, 
                                            choices=["admin", 
                                                    "user", "user_priv", 
                                                    "node", "node_priv"], 
                                            help='role')
    user_parser.add_argument("--team", type=str, required=False, 
                                            help='team')


    job_parser = servicesubparser.add_parser("job")
    job_parser.add_argument("--action", type=str, required=True, 
                                            choices=["schedule", "get", "modify", "delete"],
                                            help='action to perform on job')

    job_parser.add_argument("--jobid", type=str, required=False, 
                                            help='jobid')

    job_parser.add_argument("--nodeid", type=str, required=False, 
                                            help='nodeid on which the job is to be scheduled')
                                            
    job_parser.add_argument("--type", type=str, required=False, 
                                            choices=["cron", "atq"], 
                                            default="cron",
                                            help='type of job')      

    job_parser.add_argument("--params_mode", type=str, required=False, 
                                            choices=["docker"],
                                            default="docker",
                                            help='mode')
    job_parser.add_argument("--params_deploy", type=str, required=False, 
                                            default='params_deploy',
                                            help='deploy')
    job_parser.add_argument("--params_execute", type=str, required=False,
                                            default='params_execute', 
                                            help='execute')
    job_parser.add_argument("--params_finish", type=str, required=False,
                                            default='params_finish', 
                                            help='finish')
    
    job_parser.add_argument("--schedule", type=str, required=False, 
                                            help='schedule')
    
    job_parser.add_argument("--trigger", type=str, required=False, default=None,
                                            help='trigger')

    job_parser.add_argument("--start-date", type=str, required=False, 
                                            help='start date')
    
    job_parser.add_argument("--end-date", type=str, required=False, 
                                            help='end date')
    
    job_parser.add_argument("--length", type=int, required=False, 
                                            help='test length in seconds')

    overhead_parser = job_parser.add_mutually_exclusive_group(required=False)
    overhead_parser.add_argument('--overhead', dest='overhead', action='store_true')
    overhead_parser.add_argument('--no-overhead', dest='overhead', action='store_false')
    job_parser.set_defaults(overhead=True)

    job_parser.add_argument("--server", type=str, required=False,  
                                            default=None,
                                            help='server')

    # job_parser.add_argument("--global-config", type=str, required=False, 
    #                                         default='global_config.json',
    #                                         help='global config json')

    job_parser.add_argument("--exp-config", type=str, required=False, 
                                            default='experiment-config.yaml',
                                            help='experiment config yaml')
    # job_parser.add_argument("--exec-config", type=str, required=False, 
    #                                         help='executor config yaml')                                        


    run_parser = servicesubparser.add_parser("run")
    run_parser.add_argument("--action", type=str, required=True, 
                                choices=["get", "download", "get-scheduled"],
                                help='action to perform on run')
    run_parser.add_argument("--runid", type=str, required=False, help='runid')
    run_parser.add_argument("--jobid", type=str, required=False, help='jobid')
    run_parser.add_argument("--nodeid", type=str, required=False, help='nodeid')
    run_parser.add_argument("--time-range", type=str, required=False, help='time range')
    run_parser.add_argument("--limit", type=int, required=False, 
                                                default=3, help='limit')
    run_parser.add_argument("--global-config", type=str, required=False, 
                default='global_config.json', help='global config json file.')

    run_parser.add_argument("--local-path", type=str, required=False, 
                default='', help='local path for downloaded artifacts.')


    node_parser = servicesubparser.add_parser("node")
    node_parser.add_argument("--action", type=str, required=True, 
                                choices=["register", "get", "update", "delete", 
                                             "scavenger-set", "scavenger-unset"],
                                help='action to perform on node')

    node_parser.add_argument("--nodeid", type=str, required=False, help='nodeid')
    node_parser.add_argument("--name", type=str, required=False, help='name')
    node_parser.add_argument("--location", type=str, required=False, help='location')
    
    node_parser.add_argument("--last-active", type=str, required=False, help='last_active')
    node_parser.add_argument("--public-ip", type=str, required=False, help='public ip address of the node')
    
    node_parser.add_argument("--description", type=str, required=False, help='description')
    node_parser.add_argument("--coords", type=str, required=False, help='coords')
    node_parser.add_argument("--provider", type=str, required=False, help='provider')

    active_parser = node_parser.add_mutually_exclusive_group(required=False)
    active_parser.add_argument('--active', dest='active', action='store_true')
    active_parser.add_argument('--no-active', dest='active', action='store_false')
    node_parser.set_defaults(active=False)

    node_parser.add_argument("--active-thres", type=int, required=False, default=600,
                                            help='active threshold in seconds')

    delete_jobs_parser = node_parser.add_mutually_exclusive_group(required=False)
    delete_jobs_parser.add_argument('--delete-jobs', dest='delete_jobs', action='store_true')
    delete_jobs_parser.add_argument('--no-delete-jobs', dest='delete_jobs', action='store_false')
    node_parser.set_defaults(delete_jobs=False)


    services_parser = servicesubparser.add_parser("services")
    services_parser.add_argument("--name", type=str, required=True, 
                                            choices=["kernel"],
                                            help='name of the service')
    services_parser.add_argument("--action", type=str, required=True, 
                                            choices=["verify"],
                                            help='verify whether user has access to the service')
    services_parser.add_argument("--verify-userid", type=str, required=True, 
                                            help='userid to verify')
    
    args = parser.parse_args()

    service = args.service
    
    if not service:
        print('usage: cli [-h] {config,user,node,job,run,services} ...')
        exit(0)
    
    action = args.action

    client = LeotestClient(grpc_hostname=args.grpc_host, 
                                grpc_port=args.grpc_port,
                                userid=args.userid,
                                access_token=args.access_token,
                                jwt_access_token=args.jwt_access_token)

    if service == 'config':
        if action == 'update':
            if not args.path:
                print('useage: cli config --action=update --path=PATH_TO_JSON')
                exit(0)
            
            with open(args.path,mode='r') as f:
                config_json = f.read()
            res = client.update_config(config_json)
            print(res)

        elif action == 'get':
            res = client.get_config()
            print(res)
        else:
            print('invalid action %s' % action)

    elif service == 'user':
        if action == 'register':
            # print(args.id)
            res = client.register_user(id=args.id, 
                                name=args.name, 
                                role=args.role, 
                                team=args.team)
            print(res)

        elif action == 'get':
            res = client.get_user(id=args.id)
            print(res)
        
        elif action == 'modify':
            res = client.modify_user(id=args.id, 
                                name=args.name, 
                                role=args.role, 
                                team=args.team)
            print(res)

        elif action == 'delete':
            res = client.delete_user(id=args.id)
            print(res)
        else:
            print('invalid action %s' % action)

    elif service == 'job':
        # example: python3 cli.py job --action=schedule --jobid=test-id --nodeid=test-node --type=cron --params_mode=docker --params_deploy="random deploy" --params_execute="random execute" --params_finish="random finish" --schedule="* * * * *" --global-config="global_config.json" --exp-config="experiment-config.yaml" --exec-config="executor-config.yaml"
        if action == 'schedule':
            config = ""
            # open args.exp_config and read it 
            with open(args.exp_config, 'r') as f:
                config = f.read()
            try:
                yaml.safe_load(config)
            except yaml.YAMLError as exc:
                print('warn: %s is not a valid yaml file: %s' % (args.exp_config, exc))

            res = client.schedule_job(
                jobid=args.jobid,
                nodeid=args.nodeid,
                type_name=args.type,
                params_mode=args.params_mode,
                params_deploy=args.params_deploy,
                params_execute=args.params_execute,
                params_finish=args.params_finish,
                schedule=args.schedule, 
                start_date=args.start_date,
                end_date=args.end_date,
                length=args.length,
                overhead=args.overhead,
                server=args.server,
                trigger=args.trigger,
                # global_config=args.global_config,
                experiment_config=config)
                
            print(res)
        
        elif action == 'get':
            if args.jobid:
                # python3 cli.py job --action=get --jobid=test-id
                res = client.get_job_by_id(args.jobid)
                print(res)
            elif args.nodeid:
                # python3 cli.py job --action=get --nodeid=test-node
                res = client.get_jobs_by_nodeid(args.nodeid)
                print(res)
            elif args.userid:
                # python3 cli.py job --action=get --nodeid=test-node
                res = client.get_jobs_by_userid(args.userid)
                print(res)
            else:
                print('job get: at least one of jobid, nodeid, or userid must be specified')    
        
        elif action == 'delete':
            if args.jobid:
                # python3 cli.py job --action=delete --jobid=test-id
                res = client.delete_job_by_id(args.jobid)
                print(res)
            elif args.nodeid: 
                # python3 cli.py job --action=delete --nodeid=test-node-1
                res = client.delete_jobs_by_nodeid(args.nodeid)
                print(res)
            else:
                print('job delete: at least one of --jobid or --nodeid must be specified')

    elif service == 'run':

        runid = args.runid 
        jobid = args.jobid 
        nodeid = args.nodeid 
        time_range_args = args.time_range 
        time_range = None
        limit = args.limit
        # global_config = args.global_config
        local_path = args.local_path

        if time_range_args:
            time_range = {}
            time_range_split = time_range_args.split(',')
            time_range['start'] = time_range_split[0]
            time_range['end'] = time_range_split[1]

        if action == 'get':
            res = client.get_runs(runid=runid, 
                                jobid=jobid, 
                                nodeid=nodeid, 
                                time_range=time_range, 
                                limit=limit)
            
            print(res)
        
        elif action == 'download':
            res = client.download_runs(
                        local_path=local_path,
                        runid=runid, 
                        jobid=jobid, 
                        nodeid=nodeid, 
                        time_range=time_range, 
                        limit=limit,
                        global_config = global_config)
            
            print(res)
        
        elif action == 'get-scheduled':
            res = client.get_scheduled_runs(
                nodeid=nodeid,
                start=time_range['start'],
                end=time_range['end']
            )
            print(res)

    elif service == 'node':

        nodeid = args.nodeid
        name = args.name if args.name else None 
        location = args.location if args.location else None 
        description = args.description  if args.description else None 
        coords = args.coords if args.coords else None 
        provider = args.provider if args.provider else None 
        active = args.active
        active_thres = args.active_thres
        delete_jobs = args.delete_jobs if args.delete_jobs else False 

        last_active = args.last_active if args.last_active else None 
        public_ip = args.public_ip if args.public_ip else None 

        if action == 'register':
            """register a node"""

            res = client.register_node(nodeid, name, description, coords, location, provider)
            print(res)
        
        elif action == 'get':
            """get a node"""

            res = client.get_nodes(nodeid, location, name, provider, 
                        active, active_thres)
            print(res)
    
        elif action == 'update':
            """update a node"""

            res = client.update_node(nodeid, 
                name=name, description=description, last_active=last_active,
                coords=coords, location=location, provider=provider, public_ip=public_ip
            )
            print(res)

        elif action == 'delete':
            """delete a node"""

            res = client.delete_node(nodeid, delete_jobs)
            print(res)
        
        elif action == 'scavenger-set':
            """set scavenger mode on a node"""

            res = client.set_scavenger_status(nodeid, True)
            print(res)
        
        elif action == 'scavenger-unset':
            """unset scavenger mode on the node"""

            res = client.set_scavenger_status(nodeid, False)
            print(res)
    
    elif service == 'services':
        service_name = args.name
        if service_name == 'kernel':
            if action == 'verify':
                res = client.kernel_access(args.verify_userid)
                print(res)
    else:
        print('no such service')
    
if __name__ == "__main__":
    main()