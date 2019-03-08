#!/usr/bin/env python

import os

import MySQLdb
import MySQLdb.cursors

import json
import argparse
import logging
import datetime
import calendar

from keystoneauth1 import loading
from keystoneauth1 import session

from keystoneclient import client as keystone_client
from ceilometerclient import client as ceilometer_client

def convert_ts(ts):
    try:
        return datetime.datetime.utcfromtimestamp(float(ts)).strftime('%Y-%m-%dT%H:%M:%S.%f')
    except ValueError:
        # Sometimes its already in DT format, yeah for consistancy
        return ts

def getDBEvents(config):
    # DB/user may be panko or ceilometer depending on OS configuration
    conn = MySQLdb.connect(user=config['user'],
                    passwd=config['passwd'],
                    host=config['host'],
                    db=config['db'],
                    cursorclass=MySQLdb.cursors.DictCursor
                    )

    cursor = conn.cursor()

    if config['nostate']:
        exclude_clause = ' AND '
        new_exclude_clauses = []

        for exclude in config['skip_events']:
            new_exclude_clauses.append("et.desc != '{}'".format(exclude))

        exclude_clause += " AND ".join(new_exclude_clauses)
    else:
        exclude_clause=''

    # mysql unix_timestamp assumes local time
    dt_start = datetime.datetime.strptime(config['start'], '%Y-%m-%dT%H:%M:%S')
    dt_end = datetime.datetime.strptime(config['end'], '%Y-%m-%dT%H:%M:%S')
    ts_start = calendar.timegm(dt_start.timetuple())
    ts_end = calendar.timegm(dt_end.timetuple())

    # Get all the events
    event_query = '''
                SELECT
                    e.id AS event_id,
                    e.message_id AS message_id,
                    e.generated AS generated,
                    et.desc AS event_type
                FROM
                    event e,
                    event_type et
                WHERE
                    e.event_type_id=et.id AND
                    e.generated BETWEEN {} AND {} AND
                    et.desc != 'compute.metrics.update' {}
            '''.format(ts_start, ts_end, exclude_clause)

    logging.warning("Event Query: %s", event_query)

    cursor.execute(event_query)

    events = {}

    for row in cursor:
        _id = row['event_id']
        events[_id]={
                     'db_id': _id,
                     'message_id': row['message_id'],
                     'generated': convert_ts(row['generated']),
                     'event_type': row['event_type'],
                     'raw': {},
                     'traits': []
                    }

    # Get all the traits
    # When doing time based queries, join on the event table to limit the results
    #   and not rely on the try block below
    # The Join may be expensive??, but otherwise we would return all the traits in the DB
    # We don't add the exclude_clause for now
    trait_query = '''
                SELECT
                    tf.event_id AS event_id,
                    tf.key AS `key`,
                    tf.value AS value,
                    'float' AS trait_type
                FROM
                    trait_float tf,
                    event e
                WHERE
                    e.id = tf.event_id AND
                    e.generated BETWEEN {0} AND {1}
                UNION ALL
                    SELECT
                        ti.event_id AS event_id,
                        ti.key AS `key`,
                        ti.value AS value,
                        'integer' AS trait_type
                    FROM
                        trait_int ti,
                        event e
                    WHERE
                        e.id = ti.event_id AND
                        e.generated BETWEEN {0} AND {1}
                UNION ALL
                    SELECT
                        tt.event_id AS event_id,
                        tt.key AS `key`,
                        tt.value AS value,
                        'string' AS trait_type
                    FROM
                        trait_text tt,
                        event e
                    WHERE
                        e.id = tt.event_id AND
                        e.generated BETWEEN {0} AND {1}
                UNION ALL
                    SELECT
                        td.event_id AS event_id,
                        td.key AS `key`,
                        td.value AS value,
                        'datetime' AS trait_type
                    FROM
                        trait_datetime td,
                        event e
                    WHERE
                        e.id = td.event_id AND
                        e.generated BETWEEN {0} AND {1}
                    '''.format(ts_start, ts_end)

    logging.warning("Trait Query: %s", trait_query)

    cursor.execute(trait_query)

    # Add traits to events
    for row in cursor:
        _id = row['event_id']
        trait = {'type': row['trait_type'], 'name': row['key'], 'value': row['value']}
        # API reports datetime
        if trait['name'] in ['launched_at', 'deleted_at']:
            trait['value'] = convert_ts(trait['value'])
        try:
            events[_id]['traits'].append(trait)
        except KeyError:
            #print "Skipping: {}".format(_id)
            # OK for 2 reasons:
            #   - possible race due to 2 queries
            #   - missing exclude_clause in trait_query
            pass

    conn.close()

    return events.values()

def get_keystone_creds():
    d = {}
    d['OS_USERNAME'] = os.environ['OS_USERNAME']
    d['OS_PASSWORD'] = os.environ['OS_PASSWORD']
    d['OS_AUTH_URL'] = os.environ['OS_AUTH_URL']
    d['OS_PROJECT_NAME'] = os.environ['OS_PROJECT_NAME']
    d['OS_REGION_NAME'] = os.environ['OS_REGION_NAME']
    d['OS_PROJECT_DOMAIN_NAME'] = os.environ['OS_PROJECT_DOMAIN_NAME']
    d['OS_USER_DOMAIN_NAME'] = os.environ['OS_USER_DOMAIN_NAME']
    d['OS_IDENTITY_API_VERSION'] = os.environ['OS_IDENTITY_API_VERSION']
    d['OS_INTERFACE'] = os.environ['OS_INTERFACE']
    return d

def decodeIDs(config, events):

    keystone = keystone_client.Client(session=config['session'],interface=config['auth']['OS_INTERFACE'])

    domains = keystone.domains.list()
    doms={}

    for domain in domains:
        doms[domain.id]=domain.name

    projects = keystone.projects.list()
    pros={}

    for project in projects:
       pro={}
       pro['name']=project.name
       pro['description']=project.description
       pro['domain'] = doms.get(project.parent_id, "UNKNOWN")
       pros[project.id]=pro


    uses={}
    for domain in domains:
        users = keystone.users.list(domain=domain)
        for user in users:
           use={}
           use['name']=user.name
           uses[user.id]=use

    for event in events:
        try:
            uid = event['user_id']
        except KeyError:
            # Some service events don't have a user tied to them
            continue
        try:
            pid = event['project_id']
        except KeyError:
            # Some service events don't have a project tied to them
            continue
        uname = uses.get(uid, {})
        pname = pros.get(pid, {})
        event['user_name'] = uname.get('name', 'UNKNOWN')
        event['project_name'] = pname.get('name', 'UNKNOWN')
        event['domain'] = pname.get('domain', 'UNKNOWN')

def initKeystone(config):
    auth=get_keystone_creds()
    config['auth']=auth

    loader = loading.get_plugin_loader('password')
    keystone = loader.load_from_options(auth_url=auth['OS_AUTH_URL'],
                                    username=auth['OS_USERNAME'],
                                    password=auth['OS_PASSWORD'],
                                    project_name=auth['OS_PROJECT_NAME'],
                                    user_domain_name=auth['OS_USER_DOMAIN_NAME'],
                                    project_domain_name=auth['OS_PROJECT_DOMAIN_NAME']
                                   )


    sess = session.Session(auth=keystone)
    config['session']=sess

def getAPIEvents(config):

    ceilometer = ceilometer_client.Client(2, session=config['session'], interface=config['auth']['OS_INTERFACE'])

    query=[]

    # Start and End required
    query.append(dict(field='start_timestamp', op='ge', value='{}'.format(config['start'])))
    query.append(dict(field='end_timestamp', op='le', value='{}'.format(config['end'])))

    # Requires our second set of panko patches
    query.append(dict(field='event_type', op='eq', value='!compute.metrics.update'))
    if config['nostate']:
        for skip in config['skip_events']:
            query.append(dict(field='event_type', op='eq', value='!{}'.format(skip)))

    events = []
    for event in ceilometer.events.list(q=query, limit=100000):
        events.append(event.to_dict())

    return events

def doReadConfig(config):
    try:
        f = open(config['config_file'], 'r')
    except IOError:
        return
    else:
        newconfig = json.load(f)
        config.update(newconfig)

def main ():

    config={}

    doParseArgs(config)
    doReadConfig(config)

    logging.basicConfig(
                format='%(asctime)s [%(levelname)s] %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S',
                level=config['loglevel']
                )

    json_out = "{}/{}_{}.json".format(config['outdir'],config['start'],config['end'])

    initKeystone(config)

    if config['use_db']:
        events = getDBEvents(config)
    else:
        # Requires our first panko patch
        events = getAPIEvents(config)

    if config['collapse_traits']:
        for event in events:
            for trait in event['traits']:
                event[trait['name']] = trait['value']
            if event['traits']:
                del event['traits']

    #print json.dumps(events)

    # If debugging, sort the events by time to make diffing easier
    if config['loglevel'] == logging.WARNING:
        newevents = sorted(events, key=lambda k: k['generated'])
        events = newevents

    decodeIDs(config, events)

    with open(json_out, 'w') as outfile:
        json.dump(events, outfile, indent=2, sort_keys=True, separators=(',', ': '))


def doParseArgs(config):
    """Parse args and return a config dict"""

    parser = argparse.ArgumentParser(description='Generate accounting records for OpenStack instances', epilog='-D and -A are mutually exclusive')
    parser.add_argument("-v", "--verbose", help="output debugging information", action="store_true")
    parser.add_argument("-n", "--nostate", help="Skip state messages", action="store_true")
    parser.add_argument("-c", "--collapse-traits", help="Collapse the traits array to the top level", action="store_true")
    parser.add_argument("-C", "--config-file", help="Configuration file")
    parser.add_argument("-s", "--start", help="Start time for records", required=True)
    parser.add_argument("-e", "--end", help="End time for records", required=True)
    parser.add_argument("-o", "--outdir", help="Output directory")
    parser.add_argument("-H", "--host", help="Database host, only valid for --use-db")
    parser.add_argument("-u", "--user", help="Database user, only valid for --use-db")
    parser.add_argument("-p", "--passwd", help="Database password, only valid for --use-db")
    parser.add_argument("-d", "--db", help="Database name, only valid for --use-db")
    connection_group = parser.add_mutually_exclusive_group(required=True)
    connection_group.add_argument("-D", "--use-db", help="Use the DB directly", action="store_true")
    connection_group.add_argument("-A", "--use-api", help="Use the API", action="store_true")

    args = parser.parse_args()

    if args.use_db and args.passwd is None and not 'passwd' in config:
            parser.error("--use-db requires --passwd.")

    config['loglevel']=logging.CRITICAL
    config['user']='panko'
    config['host']='0.0.0.0'
    config['db']='panko'
    config['nostate']=False
    config['skip_events']=[]
    config['collapse_traits']=False
    config['config_file'] = '/path/to/config/file'

    if args.collapse_traits:
        config['collapse_traits']=True

    if args.config_file:
        config['config_file'] = args.config_file

    if args.start:
        config['start'] = args.start

    if args.end:
        config['end'] = args.end

    config['outdir'] = '.'
    if args.outdir:
        config['outdir'] = args.outdir

    if args.passwd:
        config['passwd'] = args.passwd

    if args.host:
        config['host'] = args.host

    if args.verbose:
        config['loglevel']=logging.WARNING

    if args.nostate:
        config['nostate']=True
        config['skip_events'].append('compute.instance.exists')

    if args.use_db:
        config['use_db']=True
    else:
        config['use_db']=False

    return config

if __name__ == "__main__":
    main()
