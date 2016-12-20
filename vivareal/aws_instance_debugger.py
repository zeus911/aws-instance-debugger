#!/bin/env python
import json
import urllib2
from pprint import pprint

import boto3
import pyping
import os
import psutil
from datetime import datetime
from time import sleep, time

import sys

pyping.core.MAX_SLEEP = 0

SOLRS = ['10.6.2.239', '10.6.2.254', '10.6.6.199', '10.6.6.210']
ZOOKEEPERS = ['10.6.4.192', '10.6.2.19', '10.6.2.20', '10.6.6.87', '10.6.6.86']
USE_UDP = True

cloudwatch = boto3.client('cloudwatch', region_name='sa-east-1')


def identity():
    try:
        identity_request = urllib2.urlopen('http://169.254.169.254/latest/dynamic/instance-identity/document', timeout=1)
        identity = json.loads(identity_request.read())
        return identity['instanceId']
    except Exception as e:
        print 'ERROR RETRIEVING INSTANCE META DATA!!!!!!!!!!!!!!!!!! %s' % e.message
        return 'lalala'


IDENTITY = identity()


def metric(name, value, min=None, max=None):
    data = {
        'MetricName': name,
        'Dimensions': [
            {
                'Name': 'InstanceId',
                'Value': IDENTITY
            },
        ],
        'Timestamp': datetime.utcnow(),
        'Value': float(value),
        }

    statistic_values = {}
    if min is not None:
        statistic_values['Minimum'] = float(min)
    if max is not None:
        statistic_values['Maximum'] = float(max)

    if statistic_values:
        statistic_values['SampleCount'] = 1
        statistic_values['Sum'] = float(value)
        data['StatisticValues'] = statistic_values
        del data['Value']

    return data


if __name__ == '__main__':
    first_run = True
    while True:
        try:
            start = time()
            metric_data = []
            hosts = {}
            while True:
                print 'Pinging hosts...'
                for solr in SOLRS:
                    host_key = 'SolrPing_%s' % solr
                    ping = pyping.ping(solr, count=5, timeout=500, udp=USE_UDP)
                    if host_key not in hosts:
                        hosts[host_key] = {'avg': 0, 'min': 1000, 'max': 0}
                    hosts[host_key]['avg'] = float(ping.avg_rtt) if float(ping.avg_rtt) > hosts[host_key]['avg'] else hosts[host_key]['avg']
                    hosts[host_key]['max'] = float(ping.max_rtt) if float(ping.max_rtt) > hosts[host_key]['max'] else hosts[host_key]['max']
                    hosts[host_key]['min'] = float(ping.min_rtt) if float(ping.min_rtt) < hosts[host_key]['min'] else hosts[host_key]['min']

                for zoo in ZOOKEEPERS:
                    host_key = 'ZKPing_%s' % zoo
                    ping = pyping.ping(zoo, count=3, timeout=500, udp=USE_UDP)
                    if host_key not in hosts:
                        hosts[host_key] = {'avg': 0, 'min': 1000, 'max': 0}
                    hosts[host_key]['avg'] = float(ping.avg_rtt) if float(ping.avg_rtt) > hosts[host_key]['avg'] else hosts[host_key]['avg']
                    hosts[host_key]['max'] = float(ping.max_rtt) if float(ping.max_rtt) > hosts[host_key]['max'] else hosts[host_key]['max']
                    hosts[host_key]['min'] = float(ping.min_rtt) if float(ping.min_rtt) < hosts[host_key]['min'] else hosts[host_key]['min']

                if time() - start < 45 and not first_run:
                    sleep(5)
                else:
                    break

            for ping_host, ping_data in hosts.iteritems():
                metric_data.append(metric(ping_host, value=ping_data['avg'], min=ping_data['min'], max=ping_data['max']))

            metric_data.append(metric('Load', os.getloadavg()[0]))
            metric_data.append(metric('Memory', psutil.virtual_memory().percent))
            metric_data.append(metric('Swap', psutil.swap_memory().percent))

            for partition in psutil.disk_partitions():
                metric_data.append(metric('Partition%s' % partition.device.replace('/', '_'), psutil.disk_usage(partition.mountpoint).percent))

            process_count = 0
            process_thread_count = dict()
            for process in psutil.process_iter():
                process_count += 1
                with process.oneshot():
                    if process.ppid() <= 5:
                        continue
                    process_name = process.name().replace(' ', '').replace('.', '_')
                    try:
                        if process_name not in process_thread_count:
                            process_thread_count[process_name] = process.num_threads()
                        else:
                            process_thread_count[process_name] += process.num_threads()
                    except psutil.AccessDenied as e:
                        print 'Access denied listing threads for %s (pid %i)' % (process_name, process.pid)
            for process_name, thread_count in process_thread_count.iteritems():
                metric_data.append(metric('Thread_%s' % process_name, thread_count))
            metric_data.append(metric('Pids', process_count))

            cpu_times = psutil.cpu_times_percent(interval=1, percpu=False)
            try:
                metric_data.append(metric('IOWait', cpu_times.iowait))
                metric_data.append(metric('Steal', cpu_times.steal))
            except:
                metric_data.append(metric('Idle', cpu_times.idle))

            try:
                # pprint(metric_data)
                response = cloudwatch.put_metric_data(Namespace='Search/EC2', MetricData=metric_data)
                print 'Sending statistics, response: %i' % response['ResponseMetadata']['HTTPStatusCode']
                first_run = False
            except Exception as e:
                print 'ERROR SENDING STATISTICS! :( %s' % e.message

            remaining = 60 - (time() - start)
            if remaining > 1:
                print 'Sleeping for %f' % remaining
                sleep(remaining)
        except KeyboardInterrupt as e:
            sys.exit(0)
        # except Exception as e:
        #     print 'ERROR!!!!!!! %s: %s' % (str(e), e.message)
