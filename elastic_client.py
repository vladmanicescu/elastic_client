from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan
##set variables
elasticProtocol = 'https'
elastichost     = '44.203.63.49'
elasticPrefix   = ''
elasticport     = '9200'
elasticUser     = 'elastic'
elasticPassword = 'ygNihb+YrdUIb=q=iY0I'
elasticIndex    = 'sms'
actions         = []
fileRecordCount = 160000
fileCounter     = 0
elasticURL = '%s://%s:%s@%s:%s/%s' % (elasticProtocol,elasticUser, elasticPassword, elastichost, elasticport, elasticPrefix  )
es = Elasticsearch([elasticURL],verify_certs=False)

output = scan(es,
    index='sms',
    #doc_type="_doc",
    size=1000,                              ### Obviously this can be increased
    query={"query": {"match_all": {}}},
)

for record in output:
    print(record)