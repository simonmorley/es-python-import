from datetime import datetime
from elasticsearch import Elasticsearch
import csv

es = Elasticsearch(
    ['127.0.0.1'],
    port=9999,
    timeout=60
)

index = "1611241000_interface_stats"
location_id = [1170]
ap_mac = ["DC-9F-DB-98-0B-AE"] 

res = es.search(index=index, body={"query": {"match_all": {}}})
res = es.search(index=index, body={
    "size": 0,
    "aggs": {
            "filtered": {
                    "filter": {
                            "bool": {
                                    "must": [{
                                            "range": {
                                                    "timestamp": {
                                                            "gte": "2017-04-02",
                                                            "lte": "2017-04-02"
                                                    }
                                            }
                                    },
                                    { 
                                            "terms": {
                                                    "location_id": location_id,
                                            }
                                    },
                                    { 
                                            "terms": {
                                                    "ap_mac": ap_mac,
                                            }
                                    }
                                    ]
                            }
                    },
                    "aggs": {
                            "timeline": {
                                    "date_histogram": {
                                            "field": "timestamp",
                                            "interval": "day"
                                    },
                                    "aggs": {
                                            "stats" : { "extended_stats" : { "field" : "snr" } }
                                    }
                            }			
                    }
            }
        }
    }
)

res = res['aggregations']['filtered']['timeline']['buckets']

f = csv.writer(open("/tmp/test.csv", "w"))
f.writerow(["ap_mac", "location_id", "timestamp", "min", "max", "avg", "count", "sum_of_sq", "stddev"])

for item in res:
    x = item["stats"]
    f.writerow([
                ap_mac[0],
                location_id[0],
                item['key'],
                x['min'],
                x['max'],
                x['avg'],
                x['count'],
                x['sum_of_squares'],
                x['std_deviation']
              ])

