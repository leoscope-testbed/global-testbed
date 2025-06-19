'''
This file contains code for trigger based scheduling.
'''

import time 
import redis
import docker 
from threading import Thread
from datetime import timedelta
import paho.mqtt.client as mqtt
import datetime
import requests

from common.utils import get_satellite_info
from common.trigger import trigger_get_tree, trigger_evaluate_tree, trigger_verify, trigger_evaluate, FIELDS
from skyfield.api import N, W, wgs84, load, EarthSatellite

import logging 

logging.basicConfig(
    level=logging.INFO, 
    format="%(asctime)s %(filename)s:%(lineno)s %(thread)d %(levelname)s %(message)s")
log = logging.getLogger(__name__)

"""
Monitor all stats from the terminal gRPC API that can be used for 
trigger based scheduling. Example output from the starlink API command:

id:                    ut01000000-00000000-001fdb33
hardware_version:      rev2_proto3
software_version:      6ac8c726-f096-45a5-9f02-c026b2a65e78.uterm.release
state:                 CONNECTED
uptime:                107401
snr:
seconds_to_first_nonempty_slot: 0.0
pop_ping_drop_rate:    0.0
downlink_throughput_bps: 58082.72265625
uplink_throughput_bps: 95646.90625
pop_ping_latency_ms:   27.619047164916992
Alerts bit field:      0
fraction_obstructed:   0.0
currently_obstructed:  False
seconds_obstructed:
obstruction_duration:
obstruction_interval:
direction_azimuth:     -3.3248794078826904
direction_elevation:   68.28795623779297
is_snr_above_noise_floor: True
"""

class LeotestGrpcMonitor:
    """
    Class for handling triggers related to various Starlink gRPC parameters.
    """    
    def __init__(self, triggerobj, dish_id, fields, hostname='mqtt'):
        self.hostname = hostname
        self.port = 1883
        self.id = dish_id
        self.fields = fields
        self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1, "leotest_grpc")
        self.client.on_connect = self.on_connect  
        self.client.on_message = self.on_message 
        self.triggerobj = triggerobj 
        # arm callbacks 
    
    def on_connect(self, client, userdata, flags, rc):  
        print("connected with result code {0}".format(str(rc))) 

    def on_message(self, client, userdata, msg):   
        # print("Message received-> " + msg.topic + " " + str(msg.payload))  

        # get attributes and the callback
        field = msg.topic.split('/')[-1] 
        payload = msg.payload.decode('utf8')
        cb = getattr(self, 'on_%s' % field, None)
        if cb:
            cb(field, payload)
        else:
            print('warn: callback not defined for "%s"' % field)


    def get_topic_status(self, field):
        return "starlink/dish_status/%s/%s" % (self.id, field)
    
    def get_topic_list(self):
        return [self.get_topic_status(field) for field in self.fields]

    def arm_callbacks(self):
        for topic in self.get_topic_list():
            self.client.subscribe(topic)

    def run(self, run_async=False):
        self.client.connect(self.hostname, self.port)
        self.arm_callbacks()

        if run_async:
            self.client.loop_start()
        else:
            self.client.loop_forever()

    # """callback methods"""

    def on_downlink_throughput_bps(self, field, payload):
        # print('field %s payload %s' % (field, payload))
        self.triggerobj.update_field(field, int(float(payload)))
    
    def on_uplink_throughput_bps(self, field, payload):
        # print('field %s payload %s' % (field, payload))
        self.triggerobj.update_field(field, int(float(payload)))
    
    def on_pop_ping_latency_ms(self, field, payload):
        # print('field %s payload %s' % (field, payload))
        self.triggerobj.update_field(field, int(float(payload)))

    def on_fraction_obstructed(self, field, payload):
        # print('field %s payload %s' % (field, payload))
        self.triggerobj.update_field(field, float(payload))

    def on_currently_obstructed(self, field, payload):
        # print('field %s payload %s' % (field, payload))
        self.triggerobj.update_field(field, bool(payload))
    
    def on_direction_azimuth(self, field, payload):
        # print('field %s payload %s' % (field, payload))
        self.triggerobj.update_field(field, float(payload))

    def on_direction_elevation(self, field, payload):
        # print('field %s payload %s' % (field, payload))
        self.triggerobj.update_field(field, float(payload))

# class LeotestSatelliteMonitor:
#     def __init__(self, triggerobj, lat, long, elevation_m, refresh_duration_mins=10):
#         self.triggerobj = triggerobj
#         self.lat = lat 
#         self.long = long 
#         self.elevation_m = elevation_m
#         self.loc = Topos(lat, long, elevation_m=elevation_m)
    
#     def predict_satellites(self):
#         sunUp = None
#         moonUp = None 
#         eclipsed = None 
#         minAlt = 20

#         params = [sunUp, moonUp, eclipsed, minAlt]
#         start = dt.datetime.now()
#         stop = start + dt.timedelta(minutes=10)

#         passes = starlinkPassPredictor(start, stop, cambridge, params=params, path=".", filename="allPasses")


class LeotestDockerNetworkMonitor:
    def __init__(self, triggerobj, name=None, label=["leotest=true"]):
        self.triggerobj = triggerobj
        self.name = name 
        self.label = label 
        self.client = docker.from_env()


    def get_stats_total(self):
        container_list = []
        try:
            if self.name:
                container_list = [self.client.containers.get(self.name)]
            else:
                filters = {
                    "label": self.label, 
                    "status": "running" 
                }
                container_list = self.client.containers.list(filters=filters)
        except:
            pass

        total_tx_bytes = 0 
        total_rx_bytes = 0
        
        for container in container_list:
            # get stats

            for stats in container.stats(decode=True, stream=True):
                
                if "networks" in stats:
                    for iface in stats["networks"]:
                        total_tx_bytes += stats["networks"][iface]["tx_bytes"]
                        total_rx_bytes += stats["networks"][iface]["rx_bytes"]
                break
        
        return total_tx_bytes, total_rx_bytes, time.time()
    
    def run(self):
        prev_tx_bytes, prev_rx_bytes, prev_time = self.get_stats_total()

        while True: 
            time.sleep(1)
            tx_bytes, rx_bytes, curr_time = self.get_stats_total()

            tx_rate = int((tx_bytes - prev_tx_bytes) / (curr_time - prev_time))
            rx_rate = int((rx_bytes - prev_rx_bytes) / (curr_time - prev_time))

            prev_tx_bytes, prev_rx_bytes, prev_time = tx_bytes, rx_bytes, curr_time

            # print('experiment tx_rate=%d bytes/sec rx_rate=%d bytes/sec ' % (tx_rate, rx_rate))
            self.triggerobj.update_field('exp_tx_rate_bytes', tx_rate)
            self.triggerobj.update_field('exp_rx_rate_bytes', rx_rate)
    
    def run_async(self):
        thread = Thread(target = self.run)
        thread.start()

class LeotestSatelliteMonitor:

    MAX_SATELLITE_DISTANCE = 1089686.4181956202
    
    def __init__(self, triggerobj, name, lat, lon, ele):
        self.triggerobj = triggerobj 

        self.gs = {
                "name": name,
                "latitude_degrees_str": lat,
                "longitude_degrees_str": lon,
                "elevation_m_float": float(ele),
            }
    
    def extract_starlink_shells(self, tle_filename, satelliteName):
        tle_file = open(tle_filename, 'r')
        Lines = tle_file.readlines()

        for i in range(0,len(Lines),3):
            tle_first_line = list(filter(None, Lines[i].strip("\n").split(" ")))[0]
            if satelliteName == tle_first_line:
                tle_second_line = list(filter(None, Lines[i+2].strip("\n").split(" ")))

                if float(tle_second_line[2]) < 53.2 and float(tle_second_line[2]) >= 53: #Inclination of Starlink shell 1 should be 53.0 degrees
                    return 1
                if float(tle_second_line[2]) < 53.5 and float(tle_second_line[2]) >= 53.2: #Inclination of Starlink shell 4 should be 53.2 degrees
                    return 4
                if float(tle_second_line[2]) < 71 and float(tle_second_line[2]) >= 70: #Inclination of Starlink shell 2 should be 70.0 degrees
                    return 2
                if float(tle_second_line[2]) < 97.9 and float(tle_second_line[2]) >= 97.6: #Inclination of Starlink shell 3 and 5 should be 97.6 degrees
                    return 3
        return 0

    def distance_between_ground_station_satellite(self, ground_station, satellite, t):
        bluffton = wgs84.latlon(float(ground_station["latitude_degrees_str"]), float(ground_station["longitude_degrees_str"]), ground_station["elevation_m_float"])
        geocentric = satellite.at(t)
        difference = satellite - bluffton
        topocentric = difference.at(t)

        alt, az, distance = topocentric.altaz()

        # return distance.m
        return (az,distance.m, alt)

    def constellation_updates(self, tle_url, shells, ground_stations, running_time):
        satellites = load.tle_file(tle_url)
        temp_list = []
        dists = []

        for gs in ground_stations:
            for sat in satellites:
                d = self.distance_between_ground_station_satellite(gs, sat, running_time)
                if d[1] <= self.MAX_SATELLITE_DISTANCE:
                    shellnum = self.extract_starlink_shells(shells, sat.name)
                    dt, leap_second = running_time.utc_datetime_and_leap_second()
                    newscs = ((str(dt).split(" ")[1]).split(":")[2]).split("+")[0]
                    date, timeN, zone = running_time.utc_strftime().split(" ")
                    year, month, day = date.split("-")
                    hour, minute, second = timeN.split(":")
                    loggedTime = str(year)+","+str(month)+","+str(day)+","+str(hour)+","+str(minute)+","+str(newscs)
                    temp_list.append([loggedTime, d[1], d[0], d[2], gs["name"], sat.name, shellnum])
                    dists.append(d[1])

        a = sorted(temp_list, key=lambda x: x[1])
        # for aa in a:
        #     print(aa)

        max_dist = max(dists)
        min_dist = min(dists)
        mean_dist = sum(dists) / len(dists)
        # print('distance max=%d min=%d mean=%d' % (max_dist, min_dist, mean_dist))
        log.info('[satellite debug] ' 
                   'updating satellite distance: max=%s min=%s mean=%s' 
                   % (max_dist, min_dist, mean_dist))
        self.triggerobj.update_field('sat_dist_max', max_dist)
        self.triggerobj.update_field('sat_dist_min', min_dist)
        self.triggerobj.update_field('sat_dist_mean', mean_dist)

    def run(self):
        # print("run this main ..")
        ts = load.timescale()
        actual_time = ts.now()
        # print(actual_time.utc_strftime())

        ground_stations = [self.gs]
        while True:
            log.info('[satellite debug] entering while loop')
            pydate = datetime.datetime.utcnow()
            t = ts.utc(int(pydate.year), int(pydate.month), int(pydate.day), int(pydate.hour), int(pydate.minute), int(pydate.second))
            tle_url, shells = get_satellite_info()
            self.constellation_updates(tle_url, shells, ground_stations, t)
            time.sleep(2)
    
    def run_async(self):
        thread = Thread(target = self.run)
        thread.start()
                

class LeotestWeatherMonitor:
    
    def __init__(self, triggerobj, api, lat, lon, api_key):
        self.triggerobj = triggerobj 
        self.api = api 
        self.lat = lat 
        self.lon = lon 
        self.api_key = api_key 
        self.url = self.api % (str(self.lat), str(self.lon), self.api_key)
    
    def run(self):

        while True:
            response = requests.get(self.url)
            data = response.json()

            if 'weather' in data:
                w = data['weather'][0]
                self.triggerobj.update_field('weather_id', w.get('id', -1))
                self.triggerobj.update_field('weather_main', w.get('main', -1))
                self.triggerobj.update_field('weather_desc', w.get('description', -1))
            

            if 'main' in data:
                m = data['main']
                self.triggerobj.update_field('weather_temp', m.get('temp', -1))    
                self.triggerobj.update_field('weather_pressure', m.get('pressure', -1))    
                self.triggerobj.update_field('weather_humidity', m.get('humidity', -1))
                self.triggerobj.update_field('weather_temp_min', m.get('temp_min', -1))
                self.triggerobj.update_field('weather_temp_max', m.get('temp_max', -1))
            
            if 'clouds' in data:
                self.triggerobj.update_field('weather_clouds', data['clouds'].get('all', -1))
            
            if 'rain' in data:
                self.triggerobj.update_field('weather_rain1h', data['rain'].get('1h', -1))
                self.triggerobj.update_field('weather_rain3h', data['rain'].get('3h', -1))
            
            if 'snow' in data:
                self.triggerobj.update_field('weather_snow1h', data['snow'].get('1h', -1))
                self.triggerobj.update_field('weather_snow3h', data['snow'].get('3h', -1))
            
            if 'visibility' in data:
                self.triggerobj.update_field('weather_visibility', data['visibility'])
            
            if 'wind' in data:
                self.triggerobj.update_field('weather_speed', data['wind'].get('speed', -1))
                self.triggerobj.update_field('weather_deg', data['wind'].get('deg', -1))
                self.triggerobj.update_field('weather_gust', data['wind'].get('gust', -1))

            # query weather data every 30mins. 
            # TODO: Make this configurable 
            time.sleep(1800)

    def run_async(self):
        thread = Thread(target = self.run)
        thread.start()


"""
Initialize trigger mode 
"""
class LeotestTriggerMode:
    def __init__(self, fields=FIELDS, history=5, mqtt_hostname='mqtt', mqtt_port=1883,
                    redis_hostname='redis', redis_port=6379):
        """init"""
        self.env = {}
        self.trigger_trees = {}
        self.history = history

        # if not verify:
        #     for trigger in self.triggers:
        #         self.trigger_trees[trigger] = trigger_get_tree(trigger)
        #         print(self.trigger_trees[trigger])
        
        for field in fields:
            self.env[field] = -1
            self.env["%s_avg" % field] = -1
            for i in range(1, history+1):
                self.env["%s_%d" % (field, i)] = -1
        
        # init queue 
        self.mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1, "leotest_trigger")
        self.mqtt_client.on_connect = self.on_queue_connect  
        self.mqtt_client.on_message = self.on_queue_message

        # connect with the queue 
        self.mqtt_client.connect(mqtt_hostname, mqtt_port)
        self.mqtt_client.loop_start()

        self.redis = redis.Redis(host=redis_hostname, port=redis_port, db=0)
    
    def on_queue_connect(self, client, userdata, flags, rc):  
        print("connected with result code {0}".format(str(rc))) 

    def on_queue_message(self, client, userdata, msg):   
        print("Message received-> " + msg.topic + " " + str(msg.payload))  
    
    def sync_triggers(self, remote_job_list, ttl_secs=30):
        """sync jobs from a remote job list"""
        pipe = self.redis.pipeline()
        pipe.delete('jobs')
        for job in remote_job_list:
            if job.trigger == None:
                continue
            pipe.lpush('jobs', job.get_jobid())
            # ttl = datetime.now() + timedelta(seconds=30)
            ttl = job.length_secs + ttl_secs
            pipe.set(job.get_jobid(), job.get_trigger(), ex=timedelta(seconds=ttl))
            # pipe.expire(name=jobid, time=ttl)

        pipe.execute()

    def get_all_triggers(self):
        """fetch a list of all triggers currently active"""
        triggers = []
        ret = self.redis.lrange('jobs', 0, -1)
        # print(ret) 
        for jobid in ret:
            jobid = jobid.decode('utf-8')
            ret = self.redis.get(jobid)
            if ret:
                triggers.append((jobid, ret.decode('utf-8')))
        
        return triggers
    
    def remove_trigger(self, jobid):
        """remove trigger"""
    
    def refresh_trigger(self, jobid):
        """refresh trigger expiry"""
    
    
    def evaluate_triggers(self):
        """evaluate here"""

        for jobid, trigger in self.get_all_triggers():
            ret, _ = trigger_evaluate(trigger, self.env)
            self.mqtt_client.publish("leotest/triggers/%s" % jobid, str(ret))
            # print("Trigger='%s' eval='%s'" % (trigger, ret))
    
    def verify_triggers(self):
        """verify triggers"""

        success = True
        err_msg = ""
        for trigger in self.triggers:
            ret, msg = trigger_verify(trigger, self.env)
            
            if not ret:
                success = False 
                err_msg = msg
                break 
        
        return (success, 'trigger: %s' % err_msg)

    def update_field(self, field, val):
        
        # update history 
        for i in range(2, self.history+1):
            self.env["%s_%d" % (field, i)] = self.env["%s_%d" % (field, i-1)]

        self.env["%s_1" % field] = self.env[field]
        self.env[field] = val

        sum = self.env[field]
        if type(sum) == int or type(sum) == float:
            for i in range(1, self.history+1):
                sum += self.env["%s_%d" % (field, i)]
        
            self.env["%s_avg" % field] = int(sum / (self.history+1))
        
        self.evaluate_triggers()


# triggers = ["(uplink_throughput_bps - exp_tx_rate_bytes*8 > 100000) & (downlink_throughput_bps - exp_rx_rate_bytes*8 > 40000)"]

# triggers = ["|pop_ping_latency_ms_1 - pop_ping_latency_ms| / pop_ping_latency_ms_1 * 100 > 60"]

# trigger = LeotestTriggerMode(fields=['uplink_throughput_bps', 
#                                     'downlink_throughput_bps', 
#                                     'exp_tx_rate_bytes', 
#                                     'exp_rx_rate_bytes',
#                                     'pop_ping_latency_ms',
#                                     'direction_azimuth',
#                                     'direction_elevation',
#                                     'currently_obstructed',
#                                     'fraction_obstructed'],
#                             triggers = triggers)

# grpcmon = LeotestGrpcMonitor(trigger, "ut01000000-00000000-001fdb33", 
#                 fields=['uplink_throughput_bps', 
#                         'downlink_throughput_bps', 
#                         'pop_ping_latency_ms',
#                         'direction_azimuth',
#                         'direction_elevation',
#                         'currently_obstructed',
#                         'fraction_obstructed'])

# dockermon = LeotestDockerNetworkMonitor(trigger)

# grpcmon.run(run_async=True)
# dockermon.run()
# subscribe.callback(on_message_print, ["starlink/dish_status/ut01000000-00000000-001fdb33/uplink_throughput_bps"], hostname="localhost")

