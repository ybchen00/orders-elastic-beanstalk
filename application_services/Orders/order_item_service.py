from application_services.base_application_resource import BaseApplicationResource

class OrderItemService(BaseApplicationResource):

    def get_links(self, resource_data):
        pass

    def __init__(self, configuration):
        super().__init__(configuration)
