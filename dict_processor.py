class dictProcessor:
    """Simple class used for processing dictionaries"""
    def __init__(self, sample_dict: dict, list_of_keys=None)-> None:
        """
        Constructor method of the dictProcessor class
        :param sample_dict: -> dictionary to be processed
        :param list_of_keys: -> keys to be kept
        """
        if list_of_keys is None:
            list_of_keys = []
        self.list_of_keys = list_of_keys
        self.sample_dict = sample_dict

    def select_desired_keys(self) -> dict:
        """
        Method that selects only the desired key value pairs from the dict, according to a list of keys
        :return:
        """
        formatted_dict :dict = {}
        for item in self.list_of_keys:
            formatted_dict[item] = self.sample_dict[item]
        return formatted_dict