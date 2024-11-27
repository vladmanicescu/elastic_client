from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan
from config import configReader as configreader
from datetime import datetime
from dict_processor import dictProcessor as dict_processor
from tabulate import tabulate
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

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

    def getData_outbound_sms(self) -> None:
        """
        Method that gets the data from elasticsearch
        :return: None
        """
        previousMonth: str =  str(int(datetime.now().strftime("%Y%m")) -1) + "01000000"
        actualMonth : str  = datetime.now().strftime("%Y%m") + "01000000"
        self.data = scan(self.elasticClient,
                          index=self.index,
                          size=1000,
                          query={  "query": {
                                   "bool": {
                                     "must": [
                                       {"match": {"TYPE": "M"}},
                                       {"wildcard": {"STATE": "Delivered*"}},
                                       {"range": {"STATE_TS": {
                                           "gt" : previousMonth,
                                           "lt":  actualMonth
                                       }}}
                                     ],
                                       "filter": [
                                           {
                                               "bool": {
                                                   "must_not": [
                                                       {
                                                           "term": {
                                                               "SRC_SOURCE_NEW": "Inbound"
                                                           }
                                                       }
                                                   ]
                                               }
                                           }
                                       ]
                                   }
                                 }
                               }
                         )

    def getData_inbound_sms(self) -> None:
        """
        Method that gets the data from elasticsearch
        :return: None
        """
        previousMonth: str =  str(int(datetime.now().strftime("%Y%m")) -1) + "01000000"
        actualMonth : str  = datetime.now().strftime("%Y%m") + "01000000"
        self.data = scan(self.elasticClient,
                          index=self.index,
                          size=1000,
                          query={  "query": {
                                   "bool": {
                                     "must": [
                                       {"match": {"TYPE": "M"}},
                                       {"wildcard": {"STATE": "Delivered*"}},
                                       {"wildcard": {"CALLING_PARTY_GT":"44*"}},
                                       {"range": {"STATE_TS": {
                                           "gt" : previousMonth,
                                           "lt":  actualMonth
                                       }}}
                                     ],
                                       "filter": [
                                           {
                                               "bool": {
                                                   "must": [
                                                       {
                                                           "term": {
                                                               "SRC_SOURCE_NEW": "Inbound"
                                                           }
                                                       }
                                                   ]
                                               }
                                           }
                                       ]
                                   }
                                 }
                               }
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
    client.getData_outbound_sms()
    desired_keys_outbound_sms = ["SRC_NAME_NEW",
                                 "DEST_MSC_Operator",
                                 "DEST_IMSI_Operator",
                                 "SINK_NAME",
                                 "Routing_Index_BT"]
    outbound_sms_dict_list: list = []
    for record in client.data:
        processor = dict_processor(record['_source'], list_of_keys=desired_keys_outbound_sms)
        processed_dict = processor.select_desired_keys()
        outbound_sms_dict_list.append(processed_dict)
    list_of_found_dicts: list = []
    aggregated_list_of_sms_dict: dict = []
    for item in outbound_sms_dict_list:
        count: int = 0
        for elem in outbound_sms_dict_list:
            if elem == item:
                count += 1
        extended_dict_outbound_sms = item.copy()
        extended_dict_outbound_sms["count"] = count
        if item not in list_of_found_dicts:
            list_of_found_dicts.append(item)
            aggregated_list_of_sms_dict.append(extended_dict_outbound_sms)
    print("##############Outbound SMS report########### ###")
    print(tabulate(aggregated_list_of_sms_dict, headers='keys', tablefmt="pretty"))


if __name__ == "__main__":
   main()