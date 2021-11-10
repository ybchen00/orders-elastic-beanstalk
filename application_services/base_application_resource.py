from abc import ABC, abstractmethod
import data_services.rdb_data_service as service


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

    def get_key_string(self, jto):
        if self._key_columns is not None and len(self._key_columns) > 0:
            result_list = [str(jto[k]) for k in self._key_columns]
            result = "_".join(result_list)
        else:
            result = None

        return result

    def get_key_columns(self):
        return self._key_columns

    def get_table_name(self):
        return self._table_name

    def get_db_name(self):
        return self._db_name

    @abstractmethod
    def validate_data(self, template):
        pass

    def get_by_template(self, template, fields=None, limit=None, offset=None):
        res = service.find_by_template(self._db_name, self._table_name, template, fields=fields, limit=limit, offset=offset)
        return res

    def create(self, transfer_json):
        invalid = self.validate_data(transfer_json)
        if not invalid:
            result = service.create(self._db_name, self._table_name, self._key_columns, transfer_json)
            if result:
                result = self._get_key(transfer_json)
                return result
        else:
            return invalid # should be an error TODO


    def update(self, key_values, transfer_json):
        template = dict(zip(self._key_columns, key_values))
        result = service.update(self._db_name, self._table_name, template, transfer_json)
        if result:
            result = self._get_key(template)

        return result

    def delete(self, transfer_json):
        result = service.delete(self._db_name, self._table_name, transfer_json)
        return result
        if result:
            return result
        else:
            return "object not foundddd" #TODO