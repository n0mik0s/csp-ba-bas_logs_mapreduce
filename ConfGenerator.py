import yaml
import pprint

class ConfGenerator:
    def __init__(self, path):
        self._pp = pprint.PrettyPrinter(indent=2)
        _path = path

        with open(_path, 'r') as _reader:
            try:
                self._cf = yaml.safe_load(_reader)
            except yaml.YAMLError as _err:
                print('ERR: [ConfGenerator:__init__]', _err)

    def _check(self):
        _hs = { 'request_timeout': 10,
                'scroll': '15s',
                'size': 500}
        _es_nodes_list = ['magneto.sdab.sn', 'beast.sdab.sn','storm.sdab.sn']
        _hadoop_namenodes_list = ['alderamin.sdab.sn', 'fomalgaut.sdab.sn']
        _conn = {   'ca_cert': 'es_cluster_cert.pem',
                    'port': 9200,
                    'use_ssl': True,
                    'verify_certs': True}

        if ('es_getdata' or 'es_putdata' or 'hdfs') not in self._cf: return False
        if ('conn' or 'index') not in self._cf['es_getdata']: return False
        if ('conn' or 'index') not in self._cf['es_putdata']: return False
        if not self._cf['es_getdata']['index']: return False
        if not self._cf['es_putdata']['index']: return False

        if not self._cf['es_getdata']['conn']: self._cf['es_getdata']['conn'] = _conn
        if not self._cf['es_putdata']['conn']: self._cf['es_putdata']['conn'] = _conn
        if 'helpers_scan' not in self._cf['es_getdata']: self._cf['es_getdata']['helpers_scan'] = _hs
        if 'nodes' not in self._cf['es_getdata']: self._cf['es_getdata']['nodes'] = _es_nodes_list
        if 'nodes' not in self._cf['es_putdata']: self._cf['es_putdata']['nodes'] = _es_nodes_list
        if 'namenodes' not in self._cf['hdfs']: self._cf['hdfs']['namenodes'] = _hadoop_namenodes_list
        if 'port' not in self._cf['hdfs']: self._cf['hdfs']['port'] = 50070
        if 'tmp_dir' not in self._cf['hdfs']: self._cf['hdfs']['root_dir'] = '/tmp/'
        if 'principal' not in self._cf['hdfs']: self._cf['hdfs']['principal'] = 'doopy@ITM.SN'

        return True

    def get(self):
        if self._check():
            #self._pp.pprint(self._cf)
            return self._cf
        else:
            #self._pp.pprint(self._cf)
            return False