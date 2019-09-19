q_err = {
    "sort": [{"@timestamp": {"order": "asc"}}],
    "query": {
        "bool": {
            "must": [{"match": {"severity": "E|P|W"}}],
            "filter": {
                "range" : {
                    "@timestamp" : {}
                }
            }
        }
    }
}

q_serv_err = {
    "sort": [{"@timestamp": {"order": "asc"}}],
    "query": {
        "bool": {
            "must": [{"match": {"severity": "ERROR|WARN|FATAL"}}],
            "filter": {
                "range": {
                    "@timestamp": {}
                }
            }
        }
    }
}

q_input = {
    "_source" : { "includes" : [ "Input_funcs" , "bascs", "hosts", "@timestamp" ]},
    "query": {
        "bool": {
            "must": [{"match": {"message": "IO.Input"}}],
            "filter": {
                "range": {
                    "@timestamp": {}
                }
            }
        }
    }
}