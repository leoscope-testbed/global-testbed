import json 
import logging
from uuid import uuid4
from croniter import croniter_range
from datetime import datetime, timedelta 
from dateutil.parser import parse as datetimeParse 
from common.utils import time_now

log = logging.getLogger(__name__)

def get_event_list_from_job_list(jobs, start, end):
    i = 0
    limit_start = datetimeParse(start)
    limit_end = datetimeParse(end)
    
    if (limit_end - limit_start).total_seconds() < 0:
        log.info('error: end must be greater than start')
        return 1, 'error: end must be greater than start', []
    
    # if (limit_end - limit_start).total_seconds() > 7*24*60*60:
    #     print('error: time range must be less than 7 days')
    #     return None 

    runs = []

    for job in jobs:
        start_time = datetimeParse(job['start'])
        end_time = datetimeParse(job['end'])

        # check overlap between date ranges 
        latest_start = max(start_time, limit_start)
        latest_end = min(end_time, limit_end)

        if latest_start < latest_end:
            if job['type'] == 'cron':
                for dt in croniter_range(latest_start, latest_end, job['schedule']):
                    run = {
                        'title': job['jobid'],
                        'id': i,
                        'start': str(dt),
                        'end': str(dt + timedelta(seconds=int(job['length']))),
                        'type': 'cron',
                        'params': job['params'],
                        'schedule': job['schedule'],
                        'length': int(job['length']),
                        'overhead': job['overhead'],
                        'trigger': job['trigger'],
						'server': job['server']
                    }
                    i=i+1
                    runs.append(run)
            elif job['type'] == 'atq':
                run = {
                    'title': job['jobid'],
                    'id': i,
                    'start': str(start_time),
                    'end': str(start_time + timedelta(seconds=int(job['length']))),
                    'type': 'atq',
                    'params': job['params'],
                    'schedule': job['schedule'],
                    'length': int(job['length']),
                    'overhead': job['overhead'],
                    'trigger': job['trigger'],
					'server': job['server']
                }
                i=i+1
                runs.append(run)
    
    return 0, 'success', runs

def check_schedule_conflict_range(start_time, length, sched):
    # conflict = False 
    # conflict_range = {'start': None, 'end': None}

    test_end = None
    end_time = start_time + timedelta(seconds=length)

    if sched['type'] == 'cron':

        # check overlap between date ranges 
        latest_start = max(start_time, datetimeParse(sched['start']))
        latest_end = min(end_time, datetimeParse(sched['end']))

        if latest_start < latest_end:
            # test_start = None 
            for dt1 in croniter_range(latest_start, latest_end, sched['cron']):
                # test_start = dt1 - timedelta(seconds=sched['length'])
                test_end = dt1 + timedelta(seconds=sched['length'])

    elif sched['type'] == 'atq':
        sched_start = datetimeParse(sched['start'])
        sched_end = datetimeParse(sched['end'])

        start_max = max(sched_start, start_time)
        end_min = min(sched_end, end_time)

        if start_max < end_min:
            # overlap 
            test_end = end_min
    return test_end

"""
We want to look for an empty sched['length'] slot till the end_date of the experiment. 
So we keep iterating over the list of already scheduled jobs to find that slot. 
"""
def find_empty_slot_till_job_end(start_time, end_time, length, sched_list):
    
    log.info('finding next empty slot: start_time=%s end_time%s, length=%s, sched_list=%s'
             % (start_time, end_time, length, sched_list))

    for sched in sched_list:
        while start_time + timedelta(seconds=length) < end_time:
            end = check_schedule_conflict_range(start_time, length, sched)

            if end:
                start_time = max(start_time, end)
                # print(start_time)
            else:
                break
    
    if start_time + timedelta(seconds=length) < end_time:
        return start_time
    else:
        return None 

def check_schedule_conflict_atq(sched1, sched2):
    conflict = False 
    conflict_range = {'start': None, 'end': None}

    start1 = datetimeParse(sched1['start'])
    # end1 = datetimeParse(sched1['end'])
    end1 = datetimeParse(sched1['start']) + timedelta(seconds=int(sched1['length']))

    start2 = datetimeParse(sched2['start'])
    # end2 = datetimeParse(sched2['end'])
    end2 = datetimeParse(sched2['start']) + timedelta(seconds=int(sched2['length']))

    start_max = max(start1, start2)
    end_min = min(end1, end2)

    if start_max <= end_min:
        conflict = True 
        conflict_range['start'] = start_max 
        conflict_range['end'] = end_min
    
    return conflict, conflict_range

def check_schedule_conflict_cron_atq(sched_atq, sched_cron):
    conflict = False 
    conflict_range = {'start': None, 'end': None}

    # check overlap between date ranges 
    latest_start = max(datetimeParse(sched_cron['start']), 
                                        datetimeParse(sched_atq['start']))
    latest_end = min(datetimeParse(sched_cron['end']), 
                     datetimeParse(sched_atq['start']) 
                     + timedelta(seconds=sched_atq['length']))

    if latest_start < latest_end:
        test_start = datetimeParse(sched_atq['start'])
        test_end = test_start + timedelta(seconds=sched_atq['length'])

        for dt2 in croniter_range(test_start, test_end, sched_cron['cron']):
            check1 = dt2 == test_end
            check2 = dt2 + timedelta(seconds=sched_cron['length']) == test_start
            check = not(check1 or check2)
            if check:
                conflict = True 
                conflict_range['start'] = test_start
                conflict_range['end'] = test_end
                break
    
    return conflict, conflict_range

def check_schedule_conflict_cron(sched1, sched2):
    conflict = False 
    conflict_range = {'start': None, 'end': None}

    # check overlap between date ranges 
    latest_start = max(datetimeParse(sched1['start']), datetimeParse(sched2['start']))
    latest_end = min(datetimeParse(sched1['end']), datetimeParse(sched2['end']))

    if latest_start < latest_end:
        for dt1 in croniter_range(latest_start, latest_end, sched1['cron']):
            test_start = dt1 - timedelta(seconds=sched1['length'])
            test_end = dt1 + timedelta(seconds=sched1['length'])

            for dt2 in croniter_range(test_start, test_end, sched2['cron']):
                check1 = dt2 == test_end
                check2 = dt2 + timedelta(seconds=sched2['length']) == dt1
                check = not(check1 or check2)
                if check:
                    conflict = True 
                    conflict_range['start'] = test_start
                    conflict_range['end'] = test_end
                    break

            if conflict:
                break 
    
    return conflict, conflict_range

def check_schedule_conflict_list(sched, sched_list):

    conflicts = []
    for esched in sched_list:
        log.info(sched)
        log.info(esched)

        if sched['type'] == 'cron' and esched['type'] == 'cron':
            conflict, conflict_range = check_schedule_conflict_cron(sched, esched)
        
        elif sched['type'] == 'atq' and esched['type'] == 'atq':
            conflict, conflict_range = check_schedule_conflict_atq(sched, esched)

        else:
            if sched['type'] == 'atq':
                conflict, conflict_range = check_schedule_conflict_cron_atq(sched, esched)
            else:
                conflict, conflict_range = check_schedule_conflict_cron_atq(esched, sched)


        if conflict:
            conflicts.append({'sched': esched, 'range': conflict_range})

    return conflicts

def check_schedule_conflict_range(start_time, length, sched):
    # conflict = False 
    # conflict_range = {'start': None, 'end': None}

    test_end = None
    end_time = start_time + timedelta(seconds=length)

    if sched['type'] == 'cron':

        # check overlap between date ranges 
        latest_start = max(start_time, datetimeParse(sched['start']))
        latest_end = min(end_time, datetimeParse(sched['end']))

        if latest_start < latest_end:
            # test_start = None 
            for dt1 in croniter_range(latest_start, latest_end, sched['cron']):
                # test_start = dt1 - timedelta(seconds=sched['length'])
                test_end = dt1 + timedelta(seconds=sched['length'])

    elif sched['type'] == 'atq':
        sched_start = datetimeParse(sched['start'])
        sched_end = datetimeParse(sched['end'])

        start_max = max(sched_start, start_time)
        end_min = min(sched_end, end_time)

        if start_max < end_min:
            # overlap 
            test_end = end_min
    return test_end

class LeotestJob:
    """Base class to define the methods associated with the jobs.
    """    
    def __init__(self, 
                jobid="", 
                nodeid="", 
                userid="",
                type="", 
                start_date="", 
                end_date="",
                length_secs=None, 
                server=None,
                overhead=True,
                trigger=None,
                job_params={},
                config=None):
        if jobid:
            self.jobid = jobid
        else:
            self.jobid = "leotest_job_%s" % (uuid4())
        
        self.nodeid = nodeid
        self.userid = userid
        self.type = type
        self.job_params = job_params
        self.start_date = start_date
        self.end_date = end_date 
        self.length_secs = length_secs
        self.overhead = overhead
        self.server = server
        self.trigger = trigger
        self.config = config 
    
    def get_jobid(self):
        return self.jobid
    
    def get_nodeid(self):
        return self.nodeid

    def get_userid(self):
        return self.userid
    
    def get_type(self):
        return self.type

    def get_job_params(self):
        return self.job_params
    
    def get_length_secs(self):
        return self.length_secs
    
    def get_trigger(self):
        return self.trigger

    def get_config(self):
        return self.config
    
    def get_start_date_obj(self):
        return datetimeParse(self.start_date)

    def get_end_date_obj(self):
        return datetimeParse(self.end_date)
    
    def document(self):
        """return dictionary representing a LeotestJob"""

    def serialize(self):
        return json.dumps(self.document())

class LeotestJobCron(LeotestJob):
    def __init__(self,
                jobid="",
                nodeid="",
                userid="",
                job_params={},  
                minute="*",
                hour="*",
                day_of_month="*",
                month="*",
                day_of_week="*",
                start_date="", 
                end_date="",
                length_secs=None,
                server=None,
                trigger=None,
                overhead=True,
                config=None):

        super().__init__(jobid=jobid, 
                        nodeid=nodeid,
                        userid=userid,
                        type="cron", 
                        job_params=job_params,
                        start_date=start_date,
                        end_date=end_date,
                        length_secs=length_secs,
                        server=server,
                        trigger=trigger,
                        overhead=overhead,
                        config=config)

        self.schedule = {
            'minute': minute,
            'hour': hour,
            'day_of_month': day_of_month, 
            'month': month,
            'day_of_week': day_of_week 
        }

    def set_schedule_cron(self, cron):
        """set cron schedule using a cron string"""

        # TODO: put checks to verify if the cron string is valid
        minute, hour, day_of_month, month, day_of_week = cron.split(" ")
        self.schedule = {
            'minute': minute,
            'hour': hour,
            'day_of_month': day_of_month, 
            'month': month,
            'day_of_week': day_of_week 
        }

    def get_cron_string(self):
        return "%s %s %s %s %s" % (
                self.schedule['minute'],
                self.schedule['hour'],
                self.schedule['day_of_month'],
                self.schedule['month'],
                self.schedule['day_of_week'])

    def document(self):
        job = {
            'id': self.jobid,
            'nodeid': self.nodeid,
            'userid': self.userid, 
            'type': self.type,
            'params': self.job_params,
            'schedule': self.schedule,
            'start_date': self.start_date,
            'end_date': self.end_date, 
            'length_secs': self.length_secs,
            'overhead': self.overhead,
            'server': self.server,
            'trigger': self.trigger,
            'config': self.config,
            'expire_at': datetimeParse(self.end_date)
        }
        return job

class LeotestJobAtq(LeotestJob):
    def __init__(self,
                jobid="",
                nodeid="",
                userid="",
                job_params={},  
                start_date="", 
                end_date="",
                length_secs=None,
                server=None,
                trigger=None,
                overhead=True,
                config=None):

        super().__init__(jobid=jobid, 
                        nodeid=nodeid,
                        userid=userid,
                        type="atq", 
                        job_params=job_params,
                        start_date=start_date,
                        end_date=end_date,
                        length_secs=length_secs,
                        server=server,
                        trigger=trigger,
                        overhead=overhead,
                        config=config)
        
        self.schedule = ''

    def get_start_time_obj(self):
        log.info('parsing %s' % self.start_date)
        return datetimeParse(self.start_date)

    def document(self):
        job = {
            'id': self.jobid,
            'nodeid': self.nodeid, 
            'userid': self.userid,
            'type': self.type,
            'params': self.job_params,
            'schedule': self.schedule,
            'start_date': self.start_date,
            'end_date': self.end_date, 
            'length_secs': self.length_secs,
            'overhead': self.overhead,
            'server': self.server,
            'trigger': self.trigger,
            'config': self.config,
            'expire_at': datetimeParse(self.end_date)
        }
        return job

class LeotestRun:
    """Class to define the structure of the job runs and associated methods.
    """    
    def __init__(self, runid, jobid, nodeid, userid,
                        start_time, end_time, last_updated, blob_url, 
                                status, status_message):
        self.runid = runid 
        self.jobid = jobid
        self.nodeid = nodeid 
        self.userid = userid
        self.start_time = start_time
        self.end_time = end_time 
        self.last_updated = last_updated
        self.blob_url = blob_url
        self.status = status 
        self.status_message = status_message
        
    def set_status(self, status, status_message):
        self.status = status
        self.status_message = status_message
    
    def document(self):
        run = {
            'runid': self.runid,
            'jobid': self.jobid,
            'nodeid': self.nodeid,
            'userid': self.userid,
            'start_time': datetimeParse(self.start_time),
            'end_time': datetimeParse(self.end_time),
            'last_updated': datetimeParse(self.last_updated),
            'blob_url': self.blob_url,
            'status': self.status,
            'status_message': self.status_message
        } 
        return run 

class LeotestTask:
    """Class to define the structure of the tasks and associated methods, jobs at a node are converted to local tasks is scheduled using ATQ or CRON.
    """ 
    def __init__(self, taskid, runid, jobid, nodeid, task_type, ttl_secs=300):
        self.taskid = taskid 
        self.runid = runid 
        self.jobid = jobid
        self.nodeid = nodeid 
        self.task_type = task_type 
        self.status = 0
        self.expire_at = time_now() + timedelta(seconds=ttl_secs)
        self.ttl_secs = ttl_secs
        
    def set_status(self, status):
        self.status = status
    
    def get_status(self):
        return self.status 

    def get_taskid(self):
        return self.taskid 
    
    def get_runid(self):
        return self.runid
    
    def get_jobid(self):
        return self.jobid 
    
    def get_nodeid(self):
        return self.nodeid 
    
    def get_ttl_secs(self):
        return self.ttl_secs
    
    def document(self):
        run = {
            'taskid': self.taskid,
            'runid': self.runid,
            'jobid': self.jobid,
            'nodeid': self.nodeid,
            'status': self.status,
            'task_type': self.task_type,
            'ttl_secs': self.ttl_secs,
            'expire_at': self.expire_at
        } 
        return run 
    
    def document_proto_compatible(self):
        run = {
            'taskid': self.taskid,
            'runid': self.runid,
            'jobid': self.jobid,
            'nodeid': self.nodeid,
            'status': self.status,
            'type': self.task_type,
            'ttl_secs': self.ttl_secs
        } 
        return run
