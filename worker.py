import elasticsearch.helpers
import json
import datetime
import ConfGenerator
import re
import mappings
import pprint
import mrjob.protocol

from mrjob.job import MRJob

class MR_es(MRJob):
    #INTERNAL_PROTOCOL = mrjob.protocol.RawProtocol

    def _es_ini(self, args):
        _args = args

        try:
            if _args['use_ssl']:
                _return = elasticsearch.Elasticsearch(
                    _args['nodes'],
                    port=_args['port'],
                    http_auth=(_args['user'] + ':' + _args['password']),
                    verify_certs=True,
                    use_ssl=True,
                    ca_certs=_args['ca_cert']
                )
            else:
                _return = elasticsearch.Elasticsearch(
                    _args['nodes'],
                    port=_args['port'],
                    http_auth=(_args['user'] + ':' + _args['password'])
                )
        except Exception as _exc:
            print('ERR: [worker:_es_ini]: Error with establishing connection with elastic cluster:', _exc)
            return False
        else:
            return _return

    def _es_get(self, args):
        _args = args
        _es_eng = self._es_ini(_args)
        _result = []

        try:
            _scan_res = elasticsearch.helpers.scan(client=_es_eng,
                                                   request_timeout=_args['request_timeout'],
                                                   query=_args['query'],
                                                   scroll=_args['scroll'],
                                                   size=_args['size'],
                                                   index=_args['pattern'],
                                                   clear_scroll=True,
                                                   raise_on_error=False)
        except Exception as _err:
            print('ERR: [worker:_es_get]', _err)
            return False
        else:
            for _doc in _scan_res:
                _result.append(_doc['_source'])

            return _result

    def _es_put(self, args):
        _args = args

        _mode = _args['mode']
        _conf = _args['conf']
        _today = _args['today']
        _js_arr = _args['js_arr']

        if _js_arr:

            _cf = ConfGenerator.ConfGenerator(path=_conf).get()

            try:
                _es_putdata_nodes = _cf['es_putdata']['nodes']

                _es_putdata_conn_ca_cert = str(_cf['es_putdata']['conn']['ca_cert'])
                _es_putdata_conn_port = _cf['es_putdata']['conn']['port']
                _es_putdata_conn_use_ssl = _cf['es_putdata']['conn']['use_ssl']
                _es_putdata_conn_verify_certs = _cf['es_putdata']['conn']['verify_certs']

                _es_putdata_index_pattern = _cf['es_putdata']['index'][_mode]['pattern']
                _es_putdata_index_user = _cf['es_putdata']['index'][_mode]['user']
                _es_putdata_index_password = _cf['es_putdata']['index'][_mode]['password']
                _es_putdata_index_shards = _cf['es_putdata']['index'][_mode]['shards']
                _es_putdata_index_replicas = _cf['es_putdata']['index'][_mode]['replicas']
            except Exception as _err:
                print('ERR: [worker:_es_put]: There is an error with property', _err, 'in the provided conf file.')
                return False
            else:
                if _mode in 'error':
                    _mappings = mappings.map_error
                elif _mode in 'input':
                    _mappings = mappings.map_input
                else:
                    print('ERR: [worker:_es_put]: There is an error with mappings.')
                    return False

                _args = {'nodes': _es_putdata_nodes,
                         'ca_cert': _es_putdata_conn_ca_cert,
                         'port': _es_putdata_conn_port,
                         'use_ssl': _es_putdata_conn_use_ssl,
                         'verify_certs': _es_putdata_conn_verify_certs,
                         'user': _es_putdata_index_user,
                         'password': _es_putdata_index_password}
                _index = _es_putdata_index_pattern + _today

                _es_eng = self._es_ini(_args)
                _body = {
                    "settings": {
                        "number_of_shards": _es_putdata_index_shards,
                        "number_of_replicas": _es_putdata_index_replicas
                    },
                    "mappings": _mappings['mappings']
                }

                try:
                    _actions = [
                        {
                            "_index": _index,
                            "_source": _js
                        }
                        for _js in _js_arr
                    ]
                except Exception as _err:
                    print('ERR: [worker:_es_put] Error with _actions:', _err)
                    pass
                else:
                    if not _es_eng.indices.exists(index=_index):
                        try:
                            _es_eng.indices.create(index=_index, body=_body)
                        except:
                            pass

                    try:
                        elasticsearch.helpers.bulk(_es_eng, _actions, chunk_size=20000, request_timeout=30)
                    except Exception as _err:
                        print('ERR: [worker:_es_put]', _err)
                        return False

    def mapper(self, _, value):
        _value = value
        _js_input = json.loads(_value)

        try:
            _query = _js_input['query']
            _mode = _js_input['mode']
            _conf = _js_input['conf']
            _ts = _js_input['ts']
        except Exception as _err:
            print('ERR: [mapper]: There is an error with property', _err, 'in the provided json.')
        else:
            _cf = ConfGenerator.ConfGenerator(path=_conf).get()

            try:
                _es_getdata_nodes = _cf['es_getdata']['nodes']

                _es_getdata_conn_ca_cert = str(_cf['es_getdata']['conn']['ca_cert'])
                _es_getdata_conn_port = _cf['es_getdata']['conn']['port']
                _es_getdata_conn_use_ssl = _cf['es_getdata']['conn']['use_ssl']
                _es_getdata_conn_verify_certs = _cf['es_getdata']['conn']['verify_certs']

                _es_getdata_index_pattern = _cf['es_getdata']['index']['pattern']
                _es_getdata_index_user = _cf['es_getdata']['index']['user']
                _es_getdata_index_password = _cf['es_getdata']['index']['password']

                _es_getdata_helpers_scan_request_timeout = _cf['es_getdata']['helpers_scan']['request_timeout']
                _es_getdata_helpers_scan_scroll = _cf['es_getdata']['helpers_scan']['scroll']
                _es_getdata_helpers_scan_size = _cf['es_getdata']['helpers_scan']['size']
            except Exception as _err:
                print('ERR: [mapper]: There is an error with property', _err, 'in the provided conf file.')
            else:
                _args = {'nodes': _es_getdata_nodes,
                         'ca_cert': _es_getdata_conn_ca_cert,
                         'port': _es_getdata_conn_port,
                         'use_ssl': _es_getdata_conn_use_ssl,
                         'verify_certs': _es_getdata_conn_verify_certs,
                         'user': _es_getdata_index_user,
                         'password': _es_getdata_index_password,
                         'query': _query,
                         'request_timeout': _es_getdata_helpers_scan_request_timeout,
                         'scroll': _es_getdata_helpers_scan_scroll,
                         'size': _es_getdata_helpers_scan_size,
                         'pattern': _es_getdata_index_pattern}
                _iter = self._es_get(_args)
                _to_reducer = {'mode': _mode, 'conf': _conf, 'ts': _ts['lt']}

                for _item in _iter:
                    try:
                        yield (_to_reducer, _item)
                    except Exception as _err:
                        print('ERR: [mapper]:', _err)
                        pass

    def reducer(self, key, values):
        _values = values
        _key = key

        try:
            _js_arr = list(_values)
        except Exception as _err:
            print('ERR: [worker:reducer]: Error with list(_values):', _err)
            pass
        else:
            _mode = _key['mode']
            _conf = _key['conf']

            _args = {'mode': _mode,
                     'conf': _conf,
                     'js_arr': _js_arr}

            if 'error' in _mode:
                _args['today'] = '{0:%Y-%m}'.format(datetime.datetime.today())
            elif 'input' in _mode:
                _args['today'] = '{0:%Y-%m-%d}'.format(datetime.datetime.today())
            else:
                print('ERR: [worker:reducer]: Please, set up the right mode.', _mode)

            self._es_put(args=_args)

if __name__ == '__main__':
    MR_es.run()