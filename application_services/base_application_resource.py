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

    def _get_key(self, jto):
        if self._key_columns is not None and len(self._key_columns) > 0:
            result = {k: jto[k] for k in self._key_columns}
        else:
            result = None

        return result

    def get_key_columns(self):
        return self._key_columns

    def get_table_name(self):
        return self._table_name

    def get_db_name(self):
        return self._db_name

    def get_by_template(self, template):
        res = service.find_by_template(self._db_name, self._table_name, template)
        return res

    def create(self, transfer_json):
        result = service.create(self._db_name, self._table_name, self._key_columns, transfer_json)
        if result:
            result = self._get_key(transfer_json)

        return result

    def update(self, key_values, transfer_json):
        template = dict(zip(self._key_columns, key_values))
        result = service.update(self._db_name, self._table_name, template, transfer_json)
        if result:
            result = self._get_key(template)

        return result

    @abstractmethod
    def get_links(self, resource_data):
        pass
