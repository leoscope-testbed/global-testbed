#!/bin/bash

# Assuming the file containing the filename is named "filename_file.txt"
config_file=$1
if [ -n $config_file ]; then
	echo "Using default config file <sanity_test.conf>"
	config_file="sanity_test.conf"
fi

config_param=`cat $config_file`
#echo "$config_param"

userid=$(echo $config_param | grep -oP 'userid=\K[^ ]+')
access_token=$(echo $config_param | grep -oP 'access-token=\K[^ ]+')

if [ -z "$userid" ]; then
	echo "No userid provided using default user"
	userid="test_user"
	access_token="test_user_access_token"
else
	access_token=$(echo $config_param | grep -oP 'access-token=\K[^ ]+')
	if [ -z "$access_token" ]; then
		echo "No access_token provided terminating the test"
		echo "userid found = $userid"
		exit
	fi
fi

#echo "userid found = $userid"
#echo "access_token found = $access_token"

jobid=$(echo $config_param | grep -oP 'jobid=\K[^ ]+')
#echo "jobid found = $jobid"
nodeid=$(echo $config_param | grep -oP 'nodeid=\K[^ ]+')
#echo "nodeid found = $nodeid"
length=$(echo $config_param | grep -oP 'length=\K[^ ]+')
#echo "length found = $length"
exp_config=$(echo $config_param | grep -oP 'exp-config=\K[^ ]+')
#echo "exp_config found = $exp_config"

curr_date="$(date +'%Y-%m-%d')"
curr_time=`date -d'5 min' +"%H:%M:%S.%3N"`
start_date="${curr_date}T${curr_time}"
#echo "Current date = $start_date"
end_date="$(date -d'3 months' +'%Y-%m-%d')"
#echo "end_date = $end_date"

command=`python3 -m cli --userid=$userid --access-token=$access_token--grpc-host=leoscope.surrey.ac.uk job --action=schedule --jobid=$jobid --nodeid=$nodeid --type=atq --start-date=$start_date --end-date=$end_date --length=$length --overhead --exp-config=$exp_config`
echo $command

echo "Waiting for results"
sleep 300

# Read results
# Define the MongoDB container name and database
container_name="global-testbed-datastore-1"
new_database="leotest"
# Check if the container is running
if docker inspect -f '{{.State.Running}}' "$container_name" &> /dev/null; then
    # Run MongoDB commands inside the container
    query="{\"jobid\": "\"$jobid"\"}"
    docker exec -it "$container_name" mongoexport --db=leotest -o db_output.txt --pretty --collection=runs -q='{"jobid": $query}' --limit=1 --quiet
    status=`docker exec -it "$container_name" bash -c "cat db_output.txt | grep -oP 'status_message\":\K[^?]+'"`
    echo "Result : " $status
else
    echo "Error: MongoDB container $container_name is not running."
fi


