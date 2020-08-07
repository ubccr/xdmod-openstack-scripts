#!/usr/bin/python

import os
import json
import argparse
import logging
import datetime
import calendar
import requests
import glob

from keystoneauth1 import loading
from keystoneauth1 import session

from keystoneclient import client as keystone_client
from novaclient import client as nova_client

def deep_compare(obj):
    if isinstance(obj, dict):
        return sorted((k, deep_compare(v)) for k, v in obj.items())
    if isinstance(obj, list):
        return sorted(deep_compare(x) for x in obj)
    else:
        return obj

def doParseArgs(config):
    """Parse args and return a config dict"""

    parser = argparse.ArgumentParser(description='Generate accounting records for OpenStack instances', epilog='-D and -A are mutually exclusive')
    parser.add_argument("-v", "--verbose", help="output debugging information", action="store_true")
    parser.add_argument("-C", "--config-file", help="Configuration file")
    parser.add_argument("-o", "--outdir", help="Output directory")

    args = parser.parse_args()


    config['loglevel']=logging.CRITICAL
    config['config_file'] = '/path/to/config.conf'

    if args.config_file:
        config['config_file'] = args.config_file

    config['outdir'] = '.'
    if args.outdir:
        config['outdir'] = args.outdir

    if args.verbose:
        config['loglevel']=logging.INFO

    return config

def doReadConfig(config):
    try:
        f = open(config['config_file'], 'r')
    except IOError:
        return
    else:
        newconfig = json.load(f)
        config.update(newconfig)

def get_keystone_creds():
    d = {}
    d['OS_USERNAME'] = os.environ.get('OS_USERNAME')
    d['OS_PASSWORD'] = os.environ.get('OS_PASSWORD')
    d['OS_AUTH_URL'] = os.environ.get('OS_AUTH_URL')
    d['OS_PROJECT_NAME'] = os.environ.get('OS_PROJECT_NAME')
    d['OS_REGION_NAME'] = os.environ.get('OS_REGION_NAME')
    d['OS_PROJECT_DOMAIN_NAME'] = os.environ.get('OS_PROJECT_DOMAIN_NAME')
    d['OS_USER_DOMAIN_NAME'] = os.environ.get('OS_USER_DOMAIN_NAME')
    d['OS_IDENTITY_API_VERSION'] = os.environ.get('OS_IDENTITY_API_VERSION')
    d['OS_INTERFACE'] = os.environ.get('OS_INTERFACE')
    return d

def getData(config):
    auth=get_keystone_creds()

    loader = loading.get_plugin_loader('password')
    keystone = loader.load_from_options(auth_url=auth['OS_AUTH_URL'],
                                    username=auth['OS_USERNAME'],
                                    password=auth['OS_PASSWORD'],
                                    project_name=auth['OS_PROJECT_NAME'],
                                    user_domain_name=auth['OS_USER_DOMAIN_NAME'],
                                    project_domain_name=auth['OS_PROJECT_DOMAIN_NAME']
                                   )

    sess = session.Session(auth=keystone)
    nova = nova_client.Client(2.1, session=sess)

    hvs=[]
    hv_status={}

    for nc in nova.hypervisors.list(detailed=True):
        hv={}
        hv['id']=nc.id
        hv['hypervisor_hostname']=nc.hypervisor_hostname
        hv['vcpus']=nc.vcpus
        hv['memory_mb']=nc.memory_mb
        hvs.append(hv)

    hv_status['hypervisors']=hvs
    ts = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    hv_status['ts']=ts
    return hv_status

def getLatestFacts(config):
    file_list = glob.glob(config['outdir'] + "//hypervisor_facts_*.json")

    if len(file_list) == 0:
        return {"hypervisors": []}

    newest = max(file_list, key=lambda d: datetime.datetime.strptime(d, "{}/hypervisor_facts_%Y-%m-%dT%H:%M:%S.json".format(config['outdir'])))

    f=open(newest, 'r')
    latest_facts=json.load(f)

    return latest_facts

def isNewData(config,data):
    latest_facts = getLatestFacts(config)
    if deep_compare(latest_facts['hypervisors']) == deep_compare(data['hypervisors']):
        logging.info("No new facts found")
        return False
    else:
        logging.info("New facts found")
        return True

def main ():

    config={}

    doParseArgs(config)
    doReadConfig(config)

    logging.basicConfig(
                format='%(asctime)s [%(levelname)s] %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S',
                level=config['loglevel']
                )


    data = getData(config)

    if isNewData(config, data):
        nowtime = datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S')
        json_out = "{}/hypervisor_facts_{}.json".format(config['outdir'], nowtime)
        with open(json_out, 'w') as outfile:
            json.dump(data, outfile, indent=2, sort_keys=True)

if __name__ == "__main__":
    main()
