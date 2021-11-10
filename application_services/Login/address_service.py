from application_services.base_application_resource import BaseApplicationResource


class UserAddressService(BaseApplicationResource):

    def __init__(self, configuration):
        super().__init__(configuration)

    def validate_data(self, transfer_json):
        if 'zipcode' in transfer_json:
            zipcode = str(transfer_json['zipcode'])
            if len(zipcode) != 5 or not zipcode.isdigit():
                return "ERROR: Zipcode is invalid."
        return None
