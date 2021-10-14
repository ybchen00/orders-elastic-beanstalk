import os
import pymysql
from application_services.Orders.product_menu_service import ProductMenuService
from application_services.Orders.order_item_service import OrderItemService

def get_service_factory(resource_collection):

    _service_factory = { "order_item": OrderItemService({"db_name": "orders",
                                                                    "table_name": "order_item",
                                                                    "key_columns": ["order_id"]}),
                                    "product_menu": ProductMenuService({"db_name": "orders",
                                                                    "table_name": "product_item",
                                                                    "key_columns": ["product_id"]})
                       }

    if resource_collection in _service_factory:
        return _service_factory[resource_collection]
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
