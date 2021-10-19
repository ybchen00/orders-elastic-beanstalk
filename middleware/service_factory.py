import os
import pymysql
from application_services.Orders.product_menu_service import ProductMenuService
from application_services.Orders.order_item_service import OrderItemService
from application_services.Login.address_service import UserAddressService
from application_services.Login.user_service import UserService
from application_services.Stores.address_service import StoreAddressService


def get_service_factory(resource_service, resource_collection):
    _service_factory = {"orders": {"order": OrderItemService({"db_name": "orders",
                                                              "table_name": "order_item",
                                                              "key_columns": ["order_id"]}),
                                   "product": ProductMenuService({"db_name": "orders",
                                                                  "table_name": "product_menu",
                                                                  "key_columns": ["product_id"]})
                                   },
                        "login": {"address": UserAddressService({"db_name": "login",
                                                                 "table_name": "address",
                                                                 "key_columns": ["address_id"]}),
                                  "user": UserService({"db_name": "login",
                                                       "table_name": "user",
                                                       "key_columns": ["userID"]}),
                                  },
                        "stores": {"address": StoreAddressService({"db_name": "stores",
                                                                   "table_name": "address",
                                                                   "key_columns": ["address_id"]}),
                                   "store": UserService({"db_name": "stores",
                                                         "table_name": "store",
                                                         "key_columns": ["store_id"]}),
                                   }  # TODO
                        }
    print(resource_service in _service_factory)
    print(_service_factory[resource_service])
    if resource_service in _service_factory and resource_collection in _service_factory[resource_service]:
        return _service_factory[resource_service][resource_collection]
    else:
        return None


def get_db_info():
    # TODO: these should really be set up
    db_host = os.environ.get("DBHOST", None)
    db_user = os.environ.get("DBUSER", None)
    db_password = os.environ.get("DBPASSWORD", None)

    if db_host is not None:
        db_info = {
            "host": db_host,
            "user": db_user,
            "password": db_password,
            "cursorclass": pymysql.cursors.DictCursor
        }
    else:
        db_info = {
            "host": "ec2-db.cz3ufbylkv7z.us-east-2.rds.amazonaws.com",
            "user": "admin",
            "password": "Columbia2021",
            "cursorclass": pymysql.cursors.DictCursor
        }

    return db_info
