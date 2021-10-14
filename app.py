from flask import Flask, Response
from flask_cors import CORS
from flask import Flask, request, render_template, jsonify
import json
import utils.rest_utils as rest_utils
from middleware.service_factory import get_service_factory as ServiceFactory
import logging

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()
logger.setLevel(logging.INFO)

app = Flask(__name__)
CORS(app)

microservice = "orders"

@app.route('/')
def hello_world():
    return '<u>Hello World!</u>'

@app.route('/api/{}/<resource_collection>'.format(microservice), methods=["GET", "POST"])
def get_resource(resource_collection):
    inputs = rest_utils.RESTContext(request)
    rest_utils.log_request("resource_by_id", inputs)
    db_service = ServiceFactory(resource_collection)
    template = dict()

    if db_service is None:
        return Response("Resource Collection {} NOT FOUND".format(resource_collection), status=404, content_type="text/plain")

    if inputs.method == "GET":
        res = db_service.get_by_template(None)

        res_json = json.dumps(res, default=str)
        rsp = Response(res_json, status=200, content_type="application/json")
        return rsp

    elif inputs.method == "POST":
        template.update(inputs.data)
        res = db_service.create(template)

    else:
        return Response("Method Not Implemented.", status=501, content_type="text/plain")


@app.route('/api/{}/<resource_collection>/<resource_id>'.format(microservice), methods=["GET", "PUT"])
def get_resource_by_id(resource_collection, resource_id):
    inputs = rest_utils.RESTContext(request)
    db_service = ServiceFactory(resource_collection)

    if inputs.method == "GET":
        key_columns = db_service.get_key_columns() # TODO: okay because key columns is len() = 1 currently with db schemas
        template = {key_columns[0]: resource_id}  # TODO: implement multiple key columns ?? do we need this??
        res = db_service.get_by_template(template)

        res_json = json.dumps(res, default=str)
        rsp = Response(res_json, status=200, content_type="application/json")
        return rsp

    else:
        return Response("not implemented.", status=501)


if __name__ == '__main__':
    app.run(host="0.0.0.0", port=5000)
