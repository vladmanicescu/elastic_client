from elasticsearch import Elasticsearch
from config import configReader as configreader
from datetime import datetime
import urllib3
import csv
import os
from abc import ABC,  abstractmethod
from split_time import TimeSplitter as timeSplitter
from get_args import get_args as getArgs

options = getArgs()
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

class elasticclient:

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
        os.getenv("ELASTIC_PASSWORD"),
        config_dict['elastic_client_config']['elastichost'],
        config_dict['elastic_client_config']['elasticport'],
        config_dict['elastic_client_config']['elasticPrefix'])
        self.prefix = config_dict["inbound_report_config"]["calling_party_prefix"]
        self.elasticclient = Elasticsearch([self.elasticURL],verify_certs=False)

    def getData_outbound_sms(self, start_date: str, end_date: str) -> None:
        """
        Method that gets the data from elasticsearch
        :return: None
        """
        self.data = self.elasticclient.search(
                          index=self.index,
                          request_timeout=600,
                          body={
                                   "_source": ["SRC_NAME_NEW", "DEST_MSC_Operator", "DEST_IMSI_Operator", "SINK_NAME", "ROUTING_INDEX"],
                                   "size" : 10000,
                                   "query": {
                                   "bool": {
                                     "must": [
                                       {"match": {"TYPE": "M"}},
                                       {"wildcard": {"STATE.keyword": "Delivered*"}},
                                       {"range": {"STATE_TS": {
                                           "gte" : start_date,
                                           "lt":  end_date
                                       }}}
                                     ],
                                       "filter": [
                                           {
                                               "bool": {
                                                   "must_not": [
                                                       {
                                                           "term": {
                                                               "SRC_NAME_NEW.keyword": "Inbound"
                                                           }
                                                       }
                                                   ]
                                               }
                                           }
                                       ]
                                   }
                                 },
                                 "aggs": {
                                   "result": {
                                     "multi_terms": {
                                       "size": 10000,
                                       "terms": [{
                                         "field": "SRC_NAME_NEW.keyword"
                                       }, {
                                         "field": "DEST_MSC_Operator.keyword"
                                       },{
                                        "field": "DEST_IMSI_Operator.keyword"
                                       },{
                                        "field": "SINK_NAME.keyword"
                                       },{
                                       "field": "ROUTING_INDEX.keyword"
                                       }]
                                     }
                                   }
                                 }
                               }

                         )

    def getData_inbound_sms(self,start_date: str, end_date: str, calling_party_gt_operator: str) -> None:
        """
        Method that gets the data from elasticsearch
        :return: None
        """
        self.data = self.elasticclient.search(
            index=self.index,
            body={"_source": ["CALLING_PARTY_GT_Operator", "STATE_TS"],
                  "query": {
                      "bool": {
                          "must": [
                              {"match": {"TYPE.keyword": "M"}},
                              {"wildcard": {"STATE.keyword": "Delivered*"}},
                              {"match": {"CALLING_PARTY_GT.Operator.keyword": calling_party_gt_operator}},
                              {"range": {"STATE_TS.keyword": {
                                  "gte": start_date,
                                  "lt": end_date
                              }}}
                          ],
                          "filter": [
                              {
                                  "bool": {
                                      "must": [
                                          {
                                              "term": {
                                                  "SRC_NAME_NEW.keyword": "Inbound"
                                              }
                                          }
                                      ]
                                  }
                              }
                          ]
                      }
                  },
                  "aggs": {
                      "result": {
                          "multi_terms": {
                              "size": 10000,
                              "terms": [{
                                  "script": {
                                      "inline": "doc['STATE_TS.keyword'].getValue().substring(0,8)"
                                  }
                              }, {
                                  "field": "CALLING_PARTY_GT_Operator.keyword"
                              }]
                          }
                      }
                  }
                  }
        )

class ReportGenerator(ABC):
    """Abstract class used as a signature for the report generation classes"""
    @abstractmethod
    def generate_header(self, headers: tuple):
        pass
    @abstractmethod
    def generate_report(self, data: list) -> None:
        pass

class OutboundReport(ReportGenerator):
    def __init__(self, report_file_path: str) -> None :
        self.report_file_path = report_file_path

    def generate_header(self, headers: tuple) -> None:
        with open(self.report_file_path, 'w') as reportFile:
            writer = csv.writer(reportFile)
            writer.writerow(headers)

    def generate_report(self, data: list) -> None:
        data = tuple(data)
        with open(self.report_file_path, 'a') as reportFile:
            writer = csv.writer(reportFile)
            writer.writerow(data)

class InboundReport(ReportGenerator):
    def __init__(self, report_file_path: str) -> None :
        self.report_file_path = report_file_path

    def generate_header(self, headers: tuple) -> None:
        with open(self.report_file_path, 'w') as reportFile:
            writer = csv.writer(reportFile)
            writer.writerow(headers)

    def generate_report(self, data: list) -> None:
        data = tuple(data)
        with open(self.report_file_path, 'a') as reportFile:
            writer = csv.writer(reportFile)
            writer.writerow(data)

    def generate_total(self, sale_rate: float) -> None:
        with open(self.report_file_path, 'r') as reportFile:
            reader = csv.reader(reportFile)
        delivered_sms_list: list = [ x[1] for x in reader ]
        sale_amount_list: list = [ x[3] for x in reader]
        delivered_sms_total: int = sum(delivered_sms_list)
        sale_amount_total: float = sum(sale_amount_list)
        total_line: tuple = ("Total",
                             delivered_sms_total,
                             sale_rate,
                             sale_amount_total)
        with open(self.report_file_path, 'a') as reportFile:
            writer = csv.writer(reportFile)
            writer.writerow(total_line)



def process_outbound_report() -> None:
    """
    Function that generates outbound report.
    :return: None
    """
    configObject: configreader = configreader()
    configObject.generateConfigObject()
    configuration: dict = configObject.configObject
    outbound_client = elasticclient(configuration)
    outbound_headers = ("SRC_NAME_NEW",
                        "DEST_MSC_Operator",
                        "DEST_IMSI_Operator",
                        "SINK_NAME",
                        "Routing_Index_BT",
                        "count"
                         )
    outboundReport = OutboundReport('./report_outbound/report.csv')
    outboundReport.generate_header(outbound_headers)
    if options.daily_one_month:
        time_ranges_object: list = timeSplitter().split_daily_prev_month()
        data_outbound_sms: list = []
        for item in time_ranges_object:
            outbound_client.getData_outbound_sms(item.start_date, item.end_date)
            outbound_sms_data = outbound_client.data['aggregations']['result']['buckets']
            for record in outbound_sms_data:
                data_outbound_sms.append({"keys": record['key'], "count": record['doc_count']})
        aggregated_data_outbound_sms_list: list = []
        for item in data_outbound_sms:
            partially_aggregated_data_outbound_sms_list: list = []
            for record in data_outbound_sms:
                record_final_count: int = 0
                if item['keys'] == record['keys']:
                    partially_aggregated_data_outbound_sms_list.append(record)
            for record in  partially_aggregated_data_outbound_sms_list:
                record_final_count += record["count"]
            per_sms_aggregated_data = []
            per_sms_aggregated_data.extend(item['keys'])
            per_sms_aggregated_data.append(record_final_count)
            if per_sms_aggregated_data not in aggregated_data_outbound_sms_list:
                aggregated_data_outbound_sms_list.append(per_sms_aggregated_data)
        for item in aggregated_data_outbound_sms_list:
            outbound_report = OutboundReport("./report_outbound/report.csv")
            outbound_report.generate_report(item)
    elif options.one_query_prev_month:
        previousMonth: str = str(int(datetime.now().strftime("%Y%m")) - 1) + "01000000"
        actualMonth: str = datetime.now().strftime("%Y%m") + "01000000"
        outbound_client.getData_outbound_sms(previousMonth, actualMonth)
        print (outbound_client.data)
        outbound_sms_data = outbound_client.data['aggregations']['result']['buckets']
        for item in outbound_sms_data:
            report_data: list = []
            report_data.extend(item['key'])
            report_data.append(item['doc_count'])
            outboundReport.generate_report(report_data)

def process_inbound_report() -> None:
    """
    Function that generates inbound report.
    :return: None
    """
    configObject: configreader = configreader()
    configObject.generateConfigObject()
    configuration: dict = configObject.configObject
    inbound_client = elasticclient(configuration)
    inbound_headers = ("Day",
                        "Delivered SMS",
                        "Sale Rate",
                        "Sale Amount"
                         )
    if options.daily_one_month:
        pass
    elif options.one_query_prev_month:
        previousMonth: str = str(int(datetime.now().strftime("%Y%m")) - 1) + "01000000"
        actualMonth: str = datetime.now().strftime("%Y%m") + "01000000"
        for operator in configuration["inbound_report_config"]:
            inboundReport = InboundReport(f'./report_inbound/{operator["calling_part_gt_operator"]}_report.csv')
            inboundReport.generate_header(inbound_headers)
            inbound_client.getData_inbound_sms(previousMonth, actualMonth, operator["calling_part_gt_operator"])
            inbound_sms_data = inbound_client.data['aggregations']['result']['buckets']
            for item in inbound_sms_data:
                report_data: list = [item['key'][0],
                                     item['doc_count'],
                                     operator['sale_rate'],
                                     float(operator['sale_rate']) * item['doc_count']]
                inboundReport.generate_report(report_data)
            inboundReport.generate_total(operator['sale_rate'])

def main():
    if options.outbound:
        process_outbound_report()
    elif options.inbound:
        process_inbound_report()


if __name__ == "__main__":
   main()
