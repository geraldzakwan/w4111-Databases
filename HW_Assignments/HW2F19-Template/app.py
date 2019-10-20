
####################################################################################################
#
# DO NOT WORRY ABOUT ANY OF THE STUFF IN THIS SECTION. THIS HELPS YOU IMPLEMENT.
#
#


# Import functions and objects the microservice needs.
# - Flask is the top-level application. You implement the application by adding methods to it.
# - Response enables creating well-formed HTTP/REST responses.
# - requests enables accessing the elements of an incoming HTTP/REST request.
#
from flask import Flask, Response, request
from datetime import datetime
import json
import src.data_service.data_table_adaptor as dta

import logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

# The convention is that a compound primary key in a path has the elements sepatayed by "_"
# For example, /batting/willite01_BOS_1960_1 maps to the primary key for batting
_key_delimiter = "_"
_host = "127.0.0.1"
_port = 5002
_api_base = "/api"

application = Flask(__name__)


def handle_args(args):
    """

    :param args: The dictionary form of request.args.
    :return: The values removed from lists if they are in a list. This is flask weirdness.
        Sometimes x=y gets represented as {'x': ['y']} and this converts to {'x': 'y'}
    """

    result = {}

    if args is not None:
        for k,v in args.items():
            if type(v) == list:
                v = v[0]
            result[k] = v

    return result

# 1. Extract the input information from the requests object.
# 2. Log the information
# 3. Return extracted information.
#
def log_and_extract_input(method, path_params=None):

    path = request.path
    args = dict(request.args)
    data = None
    headers = dict(request.headers)
    method = request.method
    url = request.url
    base_url = request.base_url

    try:
        if request.data is not None:
            data = request.json
        else:
            data = None
    except Exception as e:
        # This would fail the request in a more real solution.
        data = "You sent something but I could not get JSON out of it."

    log_message = str(datetime.now()) + ": Method " + method

    # Get rid of the weird way that Flask sometimes handles query parameters.
    args = handle_args(args)

    inputs =  {
        "path": path,
        "method": method,
        "path_params": path_params,
        "query_params": args,
        "headers": headers,
        "body": data,
        "url": url,
        "base_url": base_url
        }

    # Pull out the fields list as a separate element.
    if args and args.get('fields', None):
        fields = args.get('fields')
        fields = fields.split(",")
        del args['fields']
        inputs['fields'] = fields

    log_message += " received: \n" + json.dumps(inputs, indent=2)
    logger.debug(log_message)

    return inputs


def log_response(path, rsp):
    """

    :param path: The path parameter received.
    :param rsp: Response object
    :return:
    """
    msg = rsp
    logger.debug(str(datetime.now()) + ": \n" + str(rsp))


def get_field_list(inputs):
    return inputs.get('fields', None)

def generate_success(rsp_str=None, msg=None):
    if rsp_str:
        return Response(rsp_str, status=200, content_type="application/json")

    if not msg:
        msg = 'General resource modification succeeds'

    rsp_str = json.dumps({'msg': msg})

    return Response(rsp_str, status=200, content_type="application/json")

def generate_resource_not_found(msg=None):
    if not msg:
        msg = 'General resource not found'

    rsp_str = json.dumps({'msg': msg})

    return Response(rsp_str, status=404, content_type="application/json")

def generate_invalid(ex=None, msg=None):
    if not msg:
        msg = 'General invalid request'

    if not ex:
        ex = 'Unknown reason'

    rsp_str = json.dumps({'err_msg': msg + str(ex)})

    return Response(rsp_str, status=400, content_type="application/json")

def generate_error(ex=None, msg=None):
    """

    This used to be more complicated in previous semesters, but we simplified for fall 2019.
    Does not do much now.
    :param status_code:
    :param ex:
    :param msg:
    :return:
    """
    if not msg:
        msg = 'General internal server error'

    if not ex:
        ex = 'Unknown reason'

    rsp_str = json.dumps({'err_msg': msg + str(ex)})

    return Response(rsp_str, status=500, content_type="application/json")


####################################################################################################
#
# THESE ARE JUST SOME EXAMPLES TO HELP YOU UNDERSTAND WHAT IS GOING ON.
#
#

# This function performs a basic health check. We will flesh this out.
@application.route("/health", methods=["GET"])
def health_check():

    rsp_data = { "status": "healthy", "time": str(datetime.now()) }
    rsp_str = json.dumps(rsp_data)
    rsp = Response(rsp_str, status=200, content_type="application/json")
    return rsp


@application.route("/demo/<parameter>", methods=["GET", "PUT", "DELETE", "POST"])
def demo(parameter):
    """
    This simple echoes the various elements that you get for handling a REST request.
    Look at https://flask.palletsprojects.com/en/1.1.x/api/#incoming-request-data

    :param parameter: A list of the path parameters.
    :return: None
    """

    inputs = log_and_extract_input(demo, { "parameter": parameter })

    msg = {
        "/demo received the following inputs" : inputs
    }

    rsp = Response(json.dumps(msg), status=200, content_type="application/json")
    return rsp

####################################################################################################
#
# YOU HAVE TO COMPLETE THE IMPLEMENTATION OF THE FUNCTIONS BELOW.
#
#
@application.route("/api/databases", methods=["GET"])
def dbs():
    """

    :return: A JSON object/list containing the databases at this endpoint.
    """
    # -- TO IMPLEMENT --

    inputs = log_and_extract_input(dbs, None)

    # Your code  goes here.

    # Hint: Implement the function in data_table_adaptor
    # NOTE: I'm using get_databases() function for this

    rsp_data = tuple(dta.get_databases())
    rsp_str = json.dumps(rsp_data)
    rsp = Response(rsp_str, status=200, content_type="application/json")
    return rsp


@application.route("/api/databases/<dbname>", methods=["GET"])
def tbls(dbname):
    """

    :param dbname: The name of a database/schema
    :return: List of tables in the database.
    """

    inputs = log_and_extract_input(tbls, (dbname))

    # Your code  goes here.

    # Hint: Implement the function in data_table_adaptor
    # NOTE: I'm using get_tables() function for this

    rsp_data = tuple(dta.get_tables(dbname))
    rsp_str = json.dumps(rsp_data)
    rsp = Response(rsp_str, status=200, content_type="application/json")
    return rsp

@application.route('/api/<dbname>/<resource>/<primary_key>', methods=['GET', 'PUT', 'DELETE'])
def resource_by_id(dbname, resource, primary_key):
    """

    :param dbname: Schema/database name.
    :param resource: Table name.
    :param primary_key: Primary key in the form "col1_col2_..._coln" with the values of key columns.
    :return: Result of operations.
    """
    # Parse the incoming request into an application specific format.
    context = log_and_extract_input(resource_by_id, (dbname, resource, primary_key))

    #
    # SOME CODE GOES HERE
    #
    # -- TO IMPLEMENT --

    # Get our rdb_data_table and at the same time cache it if it doesn't exist yet
    rdb_data_table = dta.get_rdb_table(resource, dbname)

    # Split by '_' to get all the key fields
    key_fields = primary_key.split('_')

    if request.method == 'GET':
        #
        # SOME CODE GOES HERE
        #
        # -- TO IMPLEMENT --

        # Get field_list from context
        field_list = get_field_list(context)

        try:
            rsp_data = rdb_data_table.find_by_primary_key(key_fields, field_list)
        except Exception as exception:
            print(exception)
            return generate_error(ex=exception, msg='Fetch by primary key fails: ')

        if not rsp_data:
            msg = 'No entry with such primary key'
            return generate_resource_not_found(msg=msg)

        # To accommodate for datetime format, set the default to string
        rsp_str = json.dumps(rsp_data, default=str)

        return generate_success(rsp_str=rsp_str)

    elif request.method == 'DELETE':
        #
        # SOME CODE GOES HERE
        #
        # -- TO IMPLEMENT --
        try:
            rsp_data = rdb_data_table.delete_by_key(key_fields)
        except Exception as exception:
            print(exception)
            return generate_error(ex=exception, msg='Delete by primary key fails: ')

        return generate_success(msg='Rows deleted: ' + str(rsp_data))

    elif request.method == 'PUT':
        #
        # SOME CODE GOES HERE
        #
        # -- TO IMPLEMENT --
        new_values = context['body']

        try:
            rsp_data = rdb_data_table.update_by_key(key_fields, new_values)
        except Exception as exception:
            print(exception)
            return generate_error(ex=exception, msg='Update by primary key fails: ')

        if rsp_data == 0:
            is_data_found = rdb_data_table.find_by_primary_key(key_fields)

            if not is_data_found:
                msg = 'No entry with such primary key'
                return generate_resource_not_found(msg=msg)
            else:
                msg = 'The entry is found but the new values given are the same as old values'
        else:
            msg = 'The entry is succesfully updated'

        return generate_success(msg=msg)

@application.route('/api/<dbname>/<resource_name>', methods=['GET', 'POST'])
def get_resource(dbname, resource_name):
    context = log_and_extract_input(get_resource, (dbname, resource_name))

    #
    # SOME CODE GOES HERE
    #
    # -- TO IMPLEMENT --

    # Get our rdb_data_table and at the same time cache it if it doesn't exist yet
    rdb_data_table = dta.get_rdb_table(resource_name, dbname)

    if request.method == 'GET':
        #
        # SOME CODE GOES HERE
        #
        # -- TO IMPLEMENT --

        # Get template from query params
        template = context['query_params']

        # Get field_list from context
        field_list = get_field_list(context)

        try:
            rsp_data = rdb_data_table.find_by_template(template, field_list)
        except Exception as exception:
            print(exception)
            return generate_error(ex=exception, msg='Fetch by template fails: ')

        # To accommodate for datetime format, set the default datetime to string
        rsp_str = json.dumps(rsp_data, default=str)

        return generate_success(rsp_str=rsp_str)

    elif request.method == 'POST':
        #
        # SOME CODE GOES HERE
        #
        # -- TO IMPLEMENT --
        new_record = context['body']

        try:
            rsp_data = rdb_data_table.insert(new_record)
        except Exception as exception:
            return generate_error(ex=exception, msg='Insert fails: ')

        return generate_success(msg='Entry succesfully inserted')
    else:
        return generate_invalid()


@application.route('/api/<dbname>/<parent_name>/<primary_key>/<target_name>', methods=['GET'])
def get_by_path(dbname, parent_name, primary_key, target_name):

    # Do not implement

    result = " -- THANK ALY AND ARA -- "

    return result, 501, {'Content-Type': 'application/json; charset=utf-8'}




@application.route('/api/<dbname>/<parent_name>/<primary_key>/<target_name>/<target_key>',
           methods=['GET'])
def get_by_path_key(dbname, parent_name, primary_key, target_name, target_key):
    # Do not implement

    result = " -- THANK ALY AND ARA -- "

    return result, 501, {'Content-Type': 'application/json; charset=utf-8'}


# You can ignore this method.
def handle_error(e, result):
    return "Internal error.", 504, {'Content-Type': 'text/plain; charset=utf-8'}

# run the app.
if __name__ == "__main__":
    # Setting debug to True enables debug output. This line should be
    # removed before deploying a production app.


    logger.debug("Starting HW2 time: " + str(datetime.now()))
    application.debug = True
    application.run(host=_host, port=_port)