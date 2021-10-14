from application_services.base_application_resource import BaseApplicationResource
import data_services.rdb_data_service as service

class OrderItemService(BaseApplicationResource):

    def get_links(self, resource_data):
        pass

    def __init__(self, configuration):
        super().__init__(configuration)

    def get_primary_key_name(self):
        return

    @classmethod
    def get_by_template(cls, template):
        res = service.find_by_template("orders", "order_item", template, None)
        return res