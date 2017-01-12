import random
import re
from pprint import pprint

import requests
from requests import ConnectionError


class SolrCloud(object):

    def __init__(self, solr_hosts):
        if isinstance(solr_hosts, str):
            solr_hosts = [solr_hosts]
        self._solr_hosts = solr_hosts

    def _expand_solr_hosts(self, cmdline_hosts, myself):
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
                    statuses = requests.get('http://%s:8983/solr/admin/collections' % self._solr_host(), data={'action': 'CLUSTERSTATUS', 'wt': 'json'}, timeout=1)
                    for status in statuses.json()['cluster'].get('live_nodes', []):
                        live_node = status.split(':')[0]
                        if live_node != myself:
                            expanded_hosts.append(live_node)
                except ConnectionError as e:
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

    def _solr_host(self):
        return random.choice(self._solr_hosts).split(':')[0]  # fixme :(

    def list_collections(self):
        collections = requests.get('http://%s:8983/solr/admin/collections' % self._solr_host(), data={'action': 'LIST', 'wt': 'json'}, timeout=0.5)
        for collection in collections.json().get('collections', []):
            yield collection

    def core_statistics(self):
        cores = {}
        solr_host = self._solr_host()
        try:
            for collection in self.list_collections():
                core = {'bytes': 0, 'fragmentation': 0, 'docs': 0, 'docs_deleted': 0, 'docs_total': 0, 'merge_candidates': 0, 'segments': 0, 'segments_flush': 0, 'segments_merge': 0}
                segments = requests.get('http://%s:8983/solr/%s/admin/segments' % (solr_host, collection), data={'wt': 'json'}, timeout=1)
                for segment in segments.json().get('segments', {}).values():
                    core['bytes'] += segment.get('sizeInBytes', 0)
                    core['docs_deleted'] += segment.get('delCount', 0)
                    core['docs_total'] += segment.get('size', 0)
                    core['merge_candidates'] += 1 if segment.get('mergeCandidate') else 0
                    core['segments'] += 1
                    core['segments_flush'] += 1 if segment.get('source') == 'flush' else 0
                    core['segments_merge'] += 1 if segment.get('source') == 'merge' else 0
                core['docs'] = core['docs_total'] - core['docs_deleted']
                core['fragmentation'] = float(float(core['docs_deleted']) / core['docs_total']) * 100.0 if core['docs_total'] else 0
                cores[collection] = core
        except ConnectionError as e:
            print('Unable to connect to SolrCloud at %s:8983: %s' % (solr_host, e.message))
        return cores


if __name__ == '__main__':
    solr = SolrCloud('127.0.0.1')
    pprint(solr.core_statistics())
