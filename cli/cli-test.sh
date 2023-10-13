# --- User registeration test ---

echo "Registering nodes"
# user/node registeration 
python3 cli.py user --action=register --id=user-id-1 --name="Admin" --role=admin --team="Project Leapord - MSRI"
python3 cli.py user --action=register --id=user-id-2 --name="MSR test node" --role=node --team="Project Leapord - MSRI"
python3 cli.py user --action=register --id=user-id-3 --name="Surrey node" --role=node --team="University of Surrey"

echo 'Getting user-id-2'
# get a node 
python3 cli.py user --action=get --id=user-id-2

# modify a user 
echo 'Modify user-id-1'
python3 cli.py user --action=modify --id=user-id-1 --name="Admin" --role=user --team="Proj. Leopard"

echo 'Get user-id-1'
python3 cli.py user --action=get --id=user-id-1

# delete all user ids 
echo "Delete all user ids"
python3 cli.py user --action=delete --id=user-id-1
python3 cli.py user --action=delete --id=user-id-2
python3 cli.py user --action=delete --id=user-id-3

echo "Fetch all user ids"
python3 cli.py user --action=get --id=user-id-1
python3 cli.py user --action=get --id=user-id-2
python3 cli.py user --action=get --id=user-id-3
# --- Job management test --- 

echo "Schedule jobs"
# jobs on test-node-1
python3 cli.py job --action=schedule --jobid=test-id-1 --nodeid=test-node-1 --type=cron --params_mode=docker --params_deploy="random deploy" --params_execute="random execute" --params_finish="random finish" --schedule="*/6 * * * *"

python3 cli.py job --action=schedule --jobid=test-id-2 --nodeid=test-node-1 --type=cron --params_mode=docker --params_deploy="random deploy" --params_execute="random execute" --params_finish="random finish" --schedule="* 3-10 * * *"

python3 cli.py job --action=schedule --jobid=test-id-3 --nodeid=test-node-1 --type=cron --params_mode=docker --params_deploy="random deploy" --params_execute="random execute" --params_finish="random finish" --schedule="*/6 * * 2-10 *"

# jobs on test-node-2
python3 cli.py job --action=schedule --jobid=test-id-4 --nodeid=test-node-2 --type=cron --params_mode=docker --params_deploy="random deploy" --params_execute="random execute" --params_finish="random finish" --schedule="*/6 * * * *"

python3 cli.py job --action=schedule --jobid=test-id-5 --nodeid=test-node-2 --type=cron --params_mode=docker --params_deploy="random deploy" --params_execute="random execute" --params_finish="random finish" --schedule="* 3-10 * * *"

python3 cli.py job --action=schedule --jobid=test-id-6 --nodeid=test-node-2 --type=cron --params_mode=docker --params_deploy="random deploy" --params_execute="random execute" --params_finish="random finish" --schedule="*/6 * * 2-10 *"


# get all jobs on test-node-1
echo 'Getting all jobs on test-node-1'
python3 cli.py job --action=get --nodeid=test-node-1

# get all jobs on test-node-2
echo 'Getting all jobs on test-node-2'
python3 cli.py job --action=get --nodeid=test-node-2


# clear one job on test-node-2
echo 'Clearing test-id-5'
python3 cli.py job --action=delete --jobid=test-id-5

echo 'Getting all jobs on test-node-2'
python3 cli.py job --action=get --nodeid=test-node-2

echo 'Clear all jobs on test-node-1'
python3 cli.py job --action=delete --nodeid=test-node-1

echo 'Getting all jobs on test-node-1'
python3 cli.py job --action=get --nodeid=test-node-1

echo 'Clear all jobs on test-node-2'
python3 cli.py job --action=delete --nodeid=test-node-2

echo 'Getting all jobs on test-node-2'
python3 cli.py job --action=get --nodeid=test-node-2
