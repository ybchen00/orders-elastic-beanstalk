from abc import ABC, abstractmethod
import data_services.rdb_data_service as service


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

    def get_by_template(self, template):
        res = service.find_by_template(self._db_name, self.get_table_name(), template, None)
        return res

    @abstractmethod
    def get_links(self, resource_data):
        pass
