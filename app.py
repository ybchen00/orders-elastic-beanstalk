from flask import Response
from flask_cors import CORS
from flask import Flask, request
import json
import utils.rest_utils as rest_utils
from middleware.service_factory import get_service_factory
import logging

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()
logger.setLevel(logging.INFO)

app = Flask(__name__)
CORS(app)


@app.route('/')
def hello_world():
    return '<u>Welcome to Team in the Clouds!!</u>'

@app.route('/api/<microservice>/<resource_collection>', methods=["GET", "POST"])
def get_resource(microservice, resource_collection):
    inputs = rest_utils.RESTContext(request)
    rest_utils.log_request("resource_by_id", inputs)
    db_service = get_service_factory(microservice, resource_collection)
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

        res_json = json.dumps(res, default=str)
        rsp = Response(res_json, status=200, content_type="application/json")
        return rsp
    else:
        return Response("Method Not Implemented.", status=501, content_type="text/plain")



@app.route('/api/<microservice>/<resource_collection>/<resource_id>', methods=["GET", "PUT", "DELETE"])
def get_resource_by_id(microservice, resource_collection, resource_id):
    inputs = rest_utils.RESTContext(request)
    db_service = get_service_factory(microservice, resource_collection)

    template = dict()
    key_columns = db_service.get_key_columns()  # TODO: okay because key columns is len() = 1 currently with db schemas
    template = {key_columns[0]: resource_id}  # TODO: implement multiple key columns ?? do we need this??

    if inputs.method == "GET":
        res = db_service.get_by_template(template)

        res_json = json.dumps(res, default=str)
        rsp = Response(res_json, status=200, content_type="application/json")
        return rsp

    if inputs.method == "PUT":
        resource_columns = list(template.values())
        template.update(inputs.data)
        res = db_service.update(resource_columns, template)

        res_json = json.dumps(res, default=str)
        rsp = Response(res_json, status=200, content_type="application/json")
        return rsp
    else:
        return Response("Method Not Implemented.", status=501, content_type="text/plain")


@app.route("/api/<primary_table>/<key>/<lookup_table>", methods=["GET", "POST"])
def do_based_on_foreignkey(primary_table, key, lookup_table):
    return Response("Pass Not Implemented.", status=501, content_type="text/plain")
    pass
"""    
rsp = Response("INTERNAL ERROR", status=500, content_type="text/plain")

    try:
        inputs = rest_utils.RESTContext(request)
        rest_utils.log_request("resource_by_id", inputs)

        primary_service = _get_service_by_name(primary_table)
        service = _get_service_by_name(lookup_table)

        resource_columns = rest_utils.split_key_string(key)
        key_columns = primary_service.get_primary_key()
        template = dict(zip(key_columns, resource_columns))

        if inputs.method == "GET":

            if service is not None:
                res = service.find_by_template(template)

                if res is not None:
                    res = json.dumps(res, default=str)
                    rsp = Response(res, status=200, content_type="application/JSON")
                else:
                    rsp = Response("NOT FOUND", status=404, content_type="text/plain")

        elif inputs.method == "POST":
            if service is not None:
                template.update(inputs.data)
                res = service.create(template)

                hw3g = HW3Graph()
                hw3g.create_node(label=lookup_table, **template)
                if res is not None:
                    key = "_".join(res.values())
                    headers = {"location": "/api/" + lookup_table + "/" + key}
                    rsp = Response("CREATED", status=201, content_type="text/plain", headers=headers)
                else:
                    rsp = Response("UNPROCESSABLE ENTITY", status=422, content_type="text/plain")

        else:
            rsp = Response("NOT IMPLEMENTED", status=501)

    except Exception as e:
        # TODO Put a common handler to catch exceptions, log the error and return correct
        # HTTP status code.
        print("/api/<resource>, e = ", e)
        rsp = Response("INTERNAL ERROR", status=500, content_type="text/plain")

    return rsp
"""


if __name__ == '__main__':
    app.run(host="0.0.0.0", port=5000)
