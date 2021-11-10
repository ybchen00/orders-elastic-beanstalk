from application_services.base_application_resource import BaseApplicationResource


class StoreService(BaseApplicationResource):

    def __init__(self, configuration):
        super().__init__(configuration)

    def validate_data(self, template):
        return None