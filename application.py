from flask import Response
from flask_cors import CORS
from flask import Flask, request
import json
import utils.rest_utils as rest_utils
from data_services.rdb_data_service import BaseApplicationException
from middleware.service_factory import get_service_factory
import logging

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()
logger.setLevel(logging.INFO)

app = application = Flask(__name__)
CORS(app)


@app.route('/')
def hello_world():
    return '<u>Welcome to Team in the Clouds!!</u>'


@app.route('/<microservice>/<resource_collection>', methods=["GET", "POST"])
def get_resource(microservice, resource_collection):
    try:
        inputs = rest_utils.RESTContext(request)  # args, offset, limit
        # rest_utils.log_request("resource_by_id", inputs)
        db_service = get_service_factory(microservice, resource_collection)
        template = dict()

        if db_service is None:
            return Response("Resource Collection {} NOT FOUND".format(resource_collection), status=404,
                            content_type="text/plain")

        if inputs.method == "GET":
            try:
                res = db_service.get_by_template(inputs.args, fields=inputs.fields, limit=inputs.limit,
                                                 offset=inputs.offset)
                links = inputs.get_links()
                response = {"data": res,
                            "links": links}

                res = json.dumps(response, default=str)
                return Response(res, status=200, content_type="application/json")

            except BaseApplicationException as e:
                return Response(str(e), status=422, content_type="text/plain")


        elif inputs.method == "POST":
            template.update(inputs.data)

            try:
                res = db_service.create(template)
                if type(res) == str and "ERROR:" in res: # TODO: should really be an exception
                    return Response(res, status=400, content_type="text/plain")

                key_string = db_service.get_key_string(res)
                location = inputs.get_location(key_string)
                response = {"data": res,
                            "location": location}

                res = json.dumps(response, default=str)
                return Response(res, status=201, content_type="application/json")

            except BaseApplicationException as e:
                return Response(e.message, status=422, content_type="text/plain")

        else:
            return Response("Method Not Implemented.", status=501, content_type="text/plain")

    except BaseApplicationException as e:
        return Response("Server Error. " + str(e), status=500, content_type="text/plain")


@app.route('/<microservice>/<resource_collection>/<resource_id>', methods=["GET", "PUT", "DELETE"])
def get_resource_by_id(microservice, resource_collection, resource_id):
    try:
        inputs = rest_utils.RESTContext(request)
        db_service = get_service_factory(microservice, resource_collection)

        if db_service is None:
            return Response("Resource Collection {} NOT FOUND".format(resource_collection), status=404,
                            content_type="text/plain")

        # TODO: should be handling a _ key joined in order of the key columns --> handle multiple key columns rows
        key_columns = db_service.get_key_columns()  # TODO: okay because key columns is len() = 1 currently with db schemas
        template = {key_columns[0]: resource_id}  # TODO: implement multiple key columns ?? do we need this??
        resource_columns = list(template.values())

        if inputs.method == "GET":
            template.update(inputs.args)
            res = db_service.get_by_template(template, limit=inputs.limit, offset=inputs.offset)

            links = inputs.get_links()
            response = {"data": res,
                        "links": links}
            res = json.dumps(response, default=str)
            return Response(res, status=200, content_type="application/json")

        elif inputs.method == "PUT":
            template.update(inputs.data)
            res = db_service.update(resource_columns, template)

            res = json.dumps(res, default=str)
            return Response(res, status=200, content_type="application/json")

        elif inputs.method == "DELETE":
            template = dict(zip(key_columns, resource_columns))
            response = db_service.delete(template)
            res = json.dumps(response, default=str)

            if res is not None:
                # 204 status code does not allow for any response
                rsp = Response(res, status=204, content_type="application/json")
            else:
                rsp = Response(res, status=404, content_type="text/plain")
            return rsp
        else:
            return Response("Method Not Implemented.", status=501, content_type="text/plain")

    except Exception as e:
        return Response("Server Error. " + str(e), status=500, content_type="text/plain")


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
    application.run(host="0.0.0.0", port=5000)
