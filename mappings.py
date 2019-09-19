map_input = {
    "mappings": {
        "properties": {
            "@timestamp": {"type": "date", "format": "yyyy-MM-dd'T'HH:mm:ss.SSSX||yyyy-MM-dd'T'HH:mm:ss"},
            "bascs": {"type": "keyword"},
            "Input_funcs": {"type": "keyword"},
            "hosts": {"type": "keyword"},
            "lgs": {"type": "keyword"}
        }
    }
}

map_error = {
    "mappings": {
        "properties": {
            "severity": {"type": "keyword"},
            "@timestamp": {"type": "date", "format": "yyyy-MM-dd'T'HH:mm:ss.SSSX||yyyy-MM-dd'T'HH:mm:ss"},
            "error": {"type": "keyword"},
            "bascs": {"type": "keyword"},
            "general_cause": {"type": "keyword"},
            "timestamp": {"type": "date", "format": "yyyy-MM-dd'T'HH:mm:ss||yyyy-MM-dd"},
            "Input_funcs": {"type": "keyword"},
            "date": {"type": "text"},
            "errors_description": {"type": "keyword"},
            "funcs": {"type": "keyword"},
            "host": {"type": "keyword"},
            "hosts": {"type": "keyword"},
            "lgs": {"type": "keyword"},
            "message": {"type": "text"},
            "isiterror": {"type": "text"},
            "mseconds": {"type": "text"},
            "path": {"type": "text"},
            "path2": {"type": "text"},
            "port": {"type": "integer"},
            "source_host": {"type": "keyword"},
            "tags": {"type": "text"},
            "transaction_id": {"type": "text"},
            "unknown4": {"type": "text"}
        }
    }
}