import argparse
import os
import datetime
import ConfGenerator
import json
import pprint

from requests_kerberos import HTTPKerberosAuth
from hdfs.ext.kerberos import KerberosClient
from worker import MR_es
from queries import *

def hdfs_client_ini(conf):
    _conf = conf
    _url = ''
    _nodes = []

    for _node in _conf['namenodes']:
        _nodes.append('http://' + str(_node) + ':' + str(_conf['port']))
    _url = ';'.join(_nodes)

    if os.path.isfile(_conf['keytab']):
        _conf_keytab = _conf['keytab']
    else:
        _conf_keytab = str(os.path.dirname(os.path.realpath(__file__))) + os.sep + str(_conf['keytab'])

    try:
        os.environ["KRB5_CLIENT_KTNAME"] = _conf_keytab
    except Exception as _err:
        print('ERR: [initiator:hdfs_client_ini]', _err)
        return False

    try:
        _kerberos_auth = HTTPKerberosAuth(principal=_conf['principal'])
    except Exception as _err:
        print('ERR: [initiator:hdfs_client_ini]', _err)
        return False
    else:
        try:
            _client = KerberosClient(_url)
        except Exception as _err:
            print('ERR: [initiator:hdfs_client_ini]', _err)
            return False
        else:
            return _client

def timestamps():
    _timestamps = []

    for _h in range(0, 24):
        _lt = 'now/d-' + str(_h) + 'h'
        _gte = 'now/d-' + str(_h + 1) + 'h'
        _timestamps.append(dict({'lt': _lt, 'gte': _gte, "time_zone": "Europe/Kiev"}))

    return _timestamps

def queries_to_mrjob(queries):
    _queries = queries
    _result = []

    for _query in _queries:
        _timestamps = timestamps()

        for _ts in _timestamps:
            _result.append([_query, _ts])

    return _result

if __name__=="__main__":
    pp = pprint.PrettyPrinter(indent=4)
    startTime = datetime.datetime.now()

    argparser = argparse.ArgumentParser(usage='%(prog)s [options]')
    argparser.add_argument('-c', '--conf',
                           help='Set full path to the configuration file.',
                           default='conf.yml')
    argparser.add_argument('-m', '--mode',
                           help='Set mode for the script job: error or input',
                           default='error')
    argparser.add_argument('-v', '--verbose',
                           help='Set verbose run to true.',
                           action='store_true')

    args = argparser.parse_args()

    verbose = args.verbose
    os_root_dir = os.path.dirname(os.path.realpath(__file__))
    conf_path_full = str(os_root_dir) + os.sep + str(args.conf)

    cf = ConfGenerator.ConfGenerator(path=conf_path_full).get()

    if not (('es_getdata' in cf) and ('es_putdata' in cf) and ('hdfs' in cf)):
        print('ERR: [main]: Check config file: es_getdata|es_putdata|hdfs block are absent.')
        exit(1)

    arr_to_mrjob = []
    if 'error' in str(args.mode):
        queries = queries_to_mrjob([q_serv_err, q_err])
        for q_arr in queries:
            q = q_arr[0]
            ts = q_arr[1]
            q['query']['bool']['filter']['range']['@timestamp'] = ts
            arr_to_mrjob.append(json.dumps({'query': q, 'mode': 'error', 'conf': args.conf, 'ts': ts}))
    elif 'input' in str(args.mode):
        queries = queries_to_mrjob([q_input])
        for q_arr in queries:
            q = q_arr[0]
            ts = q_arr[1]
            q['query']['bool']['filter']['range']['@timestamp'] = ts
            arr_to_mrjob.append(json.dumps({'query': q, 'mode': 'input', 'conf': args.conf, 'ts': ts}))
    else:
        print('ERR: [main]: Please choose the right mode for script job.')
        exit(1)

    if arr_to_mrjob:
        try:
            hdfs_client = hdfs_client_ini(conf=cf['hdfs'])
        except Exception as err:
            print('ERR: [initiator] Failed to create the HDFS client:', err)
            exit(1)
        else:
            hdfs_input_to_mrjob = str(cf['hdfs']['tmp_dir']) + 'hdfs_files_list.txt'
            with hdfs_client.write(hdfs_input_to_mrjob, encoding='utf-8', overwrite=True) as writer:
                for js in arr_to_mrjob:
                    writer.write((str(js) + '\n'))

            hdfs_input_to_mrjob_uri = 'hdfs://nncluster' + hdfs_input_to_mrjob
            mr_job = MR_es(args=['-r',
                                 'hadoop',
                                 hdfs_input_to_mrjob_uri,
                                 '--hadoop-streaming-jar=/usr/hdp/3.1.0.0-78/hadoop-mapreduce/hadoop-streaming.jar',
                                 '--conf-path=.mrjob.conf',
                                 '--verbose'])

            with mr_job.make_runner() as runner:
                try:
                    runner.run()
                except Exception as err:
                    print('ERR: [initiator]', err)