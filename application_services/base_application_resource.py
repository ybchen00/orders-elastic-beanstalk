from abc import ABC, abstractmethod


class BaseApplicationException:

    def __init__(self):
        pass


class BaseApplicationResource(ABC):

    def __init__(self, configuration):
        self._db_name = configuration["db_name"]
        self._table_name = configuration["table_name"]
        self._key_columns = configuration["key_columns"]

    def get_key_columns(self):
        return self._key_columns

    def get_table_name(self):
        return self._table_name

    def get_db_name(self):
        return self._db_name

    @abstractmethod
    def get_links(self, resource_data):
        pass
