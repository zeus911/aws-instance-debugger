#!/bin/env python
from __future__ import print_function

import argparse
import json
import os
import re
import sys
import traceback
import urllib2
from datetime import datetime
from math import ceil
from time import sleep, time
from vivareal.solrcloud_monitor import SolrCloud

import boto3
import psutil
import pyping

pyping.core.MAX_SLEEP = 0


class Debugger(object):
    def __init__(self, solr_hosts, zookeeper_hosts, use_udp=False, extra_dimensions=[], dry_run=False):
        self.__solr_hosts = solr_hosts
        self.__zookeeper_hosts = zookeeper_hosts
        self.__dimensions = []
        self.solrcloud = SolrCloud(solr_hosts)  # fixme

        self.dry_run = dry_run

        self.solrs = []
        self.zookeepers = []

        self.cloudwatch = boto3.client('cloudwatch', region_name='sa-east-1')
        self.identity = self._identity()
        self.use_udp = use_udp
        self.extra_dimensions = self.parse_extra_dimensions(extra_dimensions)
        self.first_run = True
        self.runs = 0

    @staticmethod
    def parse_extra_dimensions(extra_dimensions):
        if not extra_dimensions:
            return {}
        return dict((k.strip(), v.strip()) for k, v in (dimension.split('=') for dimension in extra_dimensions))

    @staticmethod
    def _expand_solr_hosts(cmdline_hosts, myself):
        expanded_hosts = []
        if cmdline_hosts:
            if isinstance(cmdline_hosts, str):
                cmdline_hosts = [cmdline_hosts]
            for cmdline_host in cmdline_hosts:
                cmdline_host = cmdline_host.split(':')[0]
                if not re.search('[a-zA-Z]', cmdline_host):
                    expanded_hosts.append(cmdline_host)
                    continue
                try:
                    statuses = urllib2.urlopen('http://%s:8983/solr/admin/collections?action=CLUSTERSTATUS&wt=json' % cmdline_host, timeout=1)
                    for status in json.loads(statuses.read())['cluster'].get('live_nodes', []):
                        live_node = status.split(':')[0]
                        if live_node != myself:
                            expanded_hosts.append(live_node)
                except urllib2.URLError as e:
                    print('Unable to connect to SolrCloud at %s:8983: %s' % (cmdline_host, e.message))
                    return []
                except Exception as e:
                    print('Unable to connect to SolrCloud at %s:8983!' % cmdline_host)
                    raise e
            expanded_hosts.sort()
        if expanded_hosts:
            print('Expanding SolrCloud DNSs from %s to ip list %s' % (', '.join(cmdline_hosts), ', '.join(expanded_hosts)))
            return expanded_hosts
        return cmdline_hosts

    @staticmethod
    def _expand_zookeeper_hosts(cmdline_hosts):
        expanded_hosts = []
        if cmdline_hosts:
            if isinstance(cmdline_hosts, str):
                cmdline_hosts = [cmdline_hosts]
            for cmdline_host in cmdline_hosts:
                cmdline_host = cmdline_host.split(':')[0]
                if not re.search('[a-zA-Z]', cmdline_host):
                    expanded_hosts.append(cmdline_host.split(':')[0])
                    continue
                try:
                    statuses = urllib2.urlopen('http://%s:8080/exhibitor/v1/cluster/status' % cmdline_host, timeout=1)
                    for status in json.loads(statuses.read()):
                        expanded_hosts.append(status.get('hostname'))
                except urllib2.URLError:
                    print('Unable to connect to Exhibitoe at %s:8080: %s' % e.message)
                    return []
                except Exception as e:
                    print('Unable to connect to Exhibitor at %s:8080!' % cmdline_host)
                    print(e)
            expanded_hosts.sort()
        if expanded_hosts:
            print('Expanding Zookeeper DNSs from %s to ip list %s' % (', '.join(cmdline_hosts), ', '.join(expanded_hosts)))
            return expanded_hosts
        return cmdline_hosts

    @staticmethod
    def _identity():
        try:
            identity_request = urllib2.urlopen('http://169.254.169.254/latest/dynamic/instance-identity/document', timeout=0.5)
            identity = json.loads(identity_request.read())
            return identity['instanceId'], identity['privateIp']
        except Exception as e:
            print('ERROR RETRIEVING INSTANCE META DATA!!!!!!!!!!!!!!!!!! %s' % e.message)
            return 'localhost', '127.0.0.1'

    def _dimensions(self):
        if not self.__dimensions:
            self.__dimensions = [
                {
                    'Name': 'InstanceId',
                    'Value': self.identity[0]
                },
                {
                    'Name': 'PrivateIp',
                    'Value': self.identity[1]
                },
            ]
            for key, value in self.extra_dimensions.iteritems():
                self.__dimensions.append({
                    'Name': key,
                    'Value': value
                })
        return self.__dimensions

    def update_hosts(self):
        self.solrs = self._expand_solr_hosts(self.__solr_hosts, self.identity[1])
        self.zookeepers = self._expand_zookeeper_hosts(self.__zookeeper_hosts)

    def ping_hosts(self, seconds=55):
        start = time()
        hosts = {}
        metric_data = list()

        print('Pinging hosts! ', end="")
        while True:
            for solr in self.solrs:
                host_key = 'SolrPing_%s' % solr
                ping = pyping.ping(solr, count=5, timeout=500, udp=self.use_udp)
                if host_key not in hosts:
                    hosts[host_key] = {'avg': 0, 'min': 1000, 'max': 0}
                if ping.avg_rtt:
                    hosts[host_key]['avg'] = float(ping.avg_rtt) if float(ping.avg_rtt) > hosts[host_key]['avg'] else hosts[host_key]['avg']
                if ping.max_rtt:
                    hosts[host_key]['max'] = float(ping.max_rtt) if float(ping.max_rtt) > hosts[host_key]['max'] else hosts[host_key]['max']
                if ping.min_rtt:
                    hosts[host_key]['min'] = float(ping.min_rtt) if float(ping.min_rtt) < hosts[host_key]['min'] else hosts[host_key]['min']
            for zoo in self.zookeepers:
                host_key = 'ZKPing_%s' % zoo
                ping = pyping.ping(zoo, count=5, timeout=500, udp=self.use_udp)
                if host_key not in hosts:
                    hosts[host_key] = {'avg': 0, 'min': 1000, 'max': 0}
                if ping.avg_rtt:
                    hosts[host_key]['avg'] = float(ping.avg_rtt) if float(ping.avg_rtt) > hosts[host_key]['avg'] else hosts[host_key]['avg']
                if ping.max_rtt:
                    hosts[host_key]['max'] = float(ping.max_rtt) if float(ping.max_rtt) > hosts[host_key]['max'] else hosts[host_key]['max']
                if ping.min_rtt:
                    hosts[host_key]['min'] = float(ping.min_rtt) if float(ping.min_rtt) < hosts[host_key]['min'] else hosts[host_key]['min']
            print('.', end="")
            sys.stdout.flush()

            if time() - start < seconds and not self.first_run:
                sleep(0.5)
            else:
                break
        print()

        for ping_host, ping_data in hosts.iteritems():
            metric_data.append(self.metric(ping_host, value=ping_data['avg'], min=ping_data['min'], max=ping_data['max'], unit='Milliseconds'))

        metric_data.append(self.metric('Load', os.getloadavg()[0], unit='Percent'))
        metric_data.append(self.metric('Memory', psutil.virtual_memory().percent, unit='Percent'))
        metric_data.append(self.metric('Swap', psutil.swap_memory().percent, unit='Percent'))

        return metric_data

    def metric(self, name, value, min=None, max=None, extra_dimensions=[], unit=None):
        dimensions = list(extra_dimensions)
        dimensions.extend(list(self._dimensions()))
        data = {
            'MetricName': name,
            'Dimensions': dimensions,
            'Timestamp': datetime.utcnow(),
            'Value': float(value),
        }

        statistic_values = {}
        if min is not None:
            statistic_values['Minimum'] = float(min)
        if max is not None:
            statistic_values['Maximum'] = float(max)

        if statistic_values and min is not None and max is not None and max > min:
            statistic_values['SampleCount'] = 1
            statistic_values['Sum'] = float(value)
            data['StatisticValues'] = statistic_values
            del data['Value']
        if unit:  # and self.is_valid_unit(unit):  # fixme
            data['Unit'] = unit
        return data

    def probe(self):
        start = time()

        if self.runs % 5 == 0:
            self.update_hosts()
        self.runs += 1

        metric_data = list()

        for partition in psutil.disk_partitions():
            metric_data.append(self.metric('Disk Usage', psutil.disk_usage(partition.mountpoint).percent, extra_dimensions=[{'Name': 'Partition', 'Value': partition.device}], unit='Percent'))

        process_count = 0
        processes = dict()
        for process in psutil.process_iter():
            process_count += 1
            with process.oneshot():
                if process.ppid() <= 5:
                    continue
                process_name = '%s (%s)' % (process.name(), process.username())
                try:
                    if process_name not in processes:
                        processes[process_name] = {
                            'max_open_files': 0,
                            'open_files': 0,
                            'thread_count': 0
                        }
                    processes[process_name]['open_files'] += process.num_fds()
                    processes[process_name]['thread_count'] += process.num_threads()
                    try:
                        processes[process_name]['max_open_files'] += process.rlimit(psutil.RLIMIT_NOFILE)[1]
                    except AttributeError:
                        pass  # mac os x :(
                except psutil.AccessDenied as e:
                    print('Access denied listing threads for %s (pid %i): %s' % (process_name, process.pid, e.message))
        for process_name, data in processes.iteritems():
            extra_dimensions = [{'Name': 'Process', 'Value': process_name}]
            metric_data.append(self.metric('Max Open Files', data['max_open_files'], extra_dimensions=extra_dimensions))
            metric_data.append(self.metric('Open Files', data['open_files'], extra_dimensions=extra_dimensions))
            metric_data.append(self.metric('Threads', data['thread_count'], extra_dimensions=extra_dimensions))
        metric_data.append(self.metric('Processes', process_count))

        cpu_times = psutil.cpu_times_percent(interval=1, percpu=False)
        try:
            metric_data.append(self.metric('IOWait', cpu_times.iowait, unit='Percent'))
            metric_data.append(self.metric('Steal', cpu_times.steal, unit='Percent'))
        except:
            metric_data.append(self.metric('Idle', cpu_times.idle, unit='Percent'))

        metric_data.extend(self.ping_hosts(55 - (time() - start)))

        # SolrCloud Core Stats <3
        for core_name, segment_data in self.solrcloud.core_statistics().iteritems():
            extra_dimensions = [{'Name': 'Core', 'Value': core_name}]
            metric_data.append(self.metric('Bytes', segment_data['bytes'], extra_dimensions=extra_dimensions, unit='Bytes'))
            metric_data.append(self.metric('Documents', segment_data['docs'], extra_dimensions=extra_dimensions))
            metric_data.append(self.metric('Deleted Docs', segment_data['docs_deleted'], extra_dimensions=extra_dimensions))
            metric_data.append(self.metric('Fragmentation', segment_data['fragmentation'], extra_dimensions=extra_dimensions, unit='Percent'))
            metric_data.append(self.metric('Merge Candidates', segment_data['merge_candidates'], extra_dimensions=extra_dimensions))
            metric_data.append(self.metric('Segments', segment_data['segments'], extra_dimensions=extra_dimensions))
            metric_data.append(self.metric('Segments Flush', segment_data['segments_flush'], extra_dimensions=extra_dimensions))
            metric_data.append(self.metric('Segments Merge', segment_data['segments_merge'], extra_dimensions=extra_dimensions))
            metric_data.append(self.metric('Total Docs', segment_data['docs_total'], extra_dimensions=extra_dimensions))

        try:
            slices = int(ceil(len(metric_data) / 20.0))
            for metric_data_slice in [metric_data[i::slices] for i in range(slices)]:
                if self.dry_run:
                    print(metric_data_slice)
                else:
                    response = self.cloudwatch.put_metric_data(Namespace='Search/EC2', MetricData=metric_data_slice)
                    print('Sending statistics, response: %i' % response['ResponseMetadata']['HTTPStatusCode'])
            self.first_run = False
        except Exception as e:
            print('ERROR SENDING STATISTICS! :( %s' % e.message)

        remaining = 60 - (time() - start)
        if remaining > 1:
            print('Sleeping for %f' % remaining)
            sleep(remaining)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='EC2 Solr/Zookeeper instance debugging')
    parser.add_argument('-solr-host', metavar='SOLR_CLUSTER_NAME', type=str, required=True, help='SolrCloud cluster DNS name')
    parser.add_argument('-zookeeper-host', metavar='ZOOKEEPER_CLUSTER_NAME', type=str, required=True, help='Zookeeper Exhibitor cluster DNS name')
    parser.add_argument('--dimensions', type=str, nargs='*', help='Extra CloudWatch Dimensions. Format: Key=Value')
    parser.add_argument('--dry-run', type=bool, nargs='?', const=True, default=False, help='Print CloudWatch data to stdout instead')
    parser.add_argument('--udp', type=bool, nargs='?', const=True, default=False, help='Use UDP ping')
    args = parser.parse_args()

    debugger = Debugger(args.solr_host, args.zookeeper_host, args.udp, args.dimensions, args.dry_run)
    while True:
        try:
            debugger.probe()
        except KeyboardInterrupt:
            print('Exiting...')
            sys.stdout.flush()
            sys.exit(0)
        except Exception as e:
            print('Error: %s' % e.message)
            traceback.print_exc(file=sys.stdout)
        sys.stdout.flush()
