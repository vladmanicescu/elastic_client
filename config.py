import yaml
import unittest

class configReader:
    def __init__(self, config_path:str = './config/config.yaml') -> None:
        """
        Constructor of the config reader class that generates a config object containing all the
        parameters that are needed and defined in a yaml config file that should be stored under
        ./config/config.yaml
        :param config_path: str -> path to config file
        """
        self.config_path = config_path
        self.configObject = None

    def generateConfigObject(self) -> None:
        """
        Method that populates the config object containing all needed parameters
        """
        with open (self.config_path, 'r') as f:
            self.configObject = yaml.safe_load(f)

class TestConfigGenerator(unittest.TestCase):
    """
    Class that implements unit testing
    """

    def test_main_config_field(self) -> None:
        """
        Method that tests that the main generated config field is as expected
        """
        tested_object = configReader()
        tested_object.generateConfigObject()
        self.assertEqual(list(tested_object.configObject.keys()), ['elastic_client_config', 'query_config'])

    def test_subfields_in_main_config_field(self) -> None:
        """
        Method that tests the subfields of the main field
        """
        tested_object = configReader()
        tested_object.generateConfigObject()
        self.assertEqual(list(tested_object.configObject["elastic_client_config"].keys()), ['elastic_protocol',
                                                                                                    'elastichost',
                                                                                                    'elasticPrefix',
                                                                                                    'elasticport',
                                                                                                    'elasticUser',
                                                                                                    'elasticPassword',
                                                                                                    'elasticIndex',
                                                                                                    'actions',
                                                                                                    'fileRecordCount',
                                                                                                    'fileCounter'
                                                                                                   ])


if __name__ == '__main__':
    unittest.main()
