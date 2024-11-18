from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan
from config import configReader as configreader

class elasticClient:

    def __init__(self, config_dict: dict) -> None:
        """
        Constructor of the elastic client class which abstracts the classes offered by the elasticsearch module
        adding a way to pass parameters via a dict object generated from a yaml config file
        :param config_dict: dict -> dictionary containing the parameters to be used
        """
        self.data = None
        self.index = config_dict['query_config']['index']
        self.elasticURL = '%s://%s:%s@%s:%s/%s' % (
        config_dict['elastic_client_config']['elastic_protocol'],
        config_dict['elastic_client_config']['elasticUser'],
        config_dict['elastic_client_config']['elasticPassword'],
        config_dict['elastic_client_config']['elastichost'],
        config_dict['elastic_client_config']['elasticport'],
        config_dict['elastic_client_config']['elasticPrefix'])
        self.elasticClient = Elasticsearch([self.elasticURL],verify_certs=False)

    def getData(self) -> None:
        """
        Method that gets the data from elasticsearch
        :return: None
        """
        self.data = scan(self.elasticClient,
                          index=self.index,
                          #doc_type="_doc",
                          size=1000,
                          query={"query": {"match_all": {}}},
                         )

def main() -> None:
    """
    Main function of the program.
    :return:
    """
    configObject: configreader = configreader()
    configObject.generateConfigObject()
    configuration: dict = configObject.configObject
    client = elasticClient(configuration)
    client.getData()
    for record in client.data:
        print(record['_source'])

if __name__ == "__main__":
   main()