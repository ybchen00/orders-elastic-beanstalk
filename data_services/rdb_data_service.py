import pymysql
import logging

import middleware.service_factory as service_factory

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()
logger.setLevel(logging.INFO)


class BaseApplicationException(Exception): # TODO should be in base_application_resource and errors should only be in there

    def __init__(self, message):
        self.message = message
        super().__init__(self.message)

    def __str__(self):
        return self.message

def _get_db_connection():
    db_connect_info = service_factory.get_db_info()

    logger.info("RDBService._get_db_connection:")
    logger.info("\t HOST = " + db_connect_info['host'])

    db_info = service_factory.get_db_info()
    db_connection = pymysql.connect(
        **db_info
    )
    return db_connection


def template_to_where_clause(t):
    s = ""

    if t is None:
        return s

    for (k, v) in t.items():
        if s != "":
            s += " and "
        s += k + "='" + v + "'"

    if s != "":
        s = "where " + s

    return s


def transfer_json_to_set_clause(t_json):
    args = []
    terms = []

    for k, v in t_json.items():
        args.append(v)
        terms.append(k + "=%s")

    clause = "set " + ", ".join(terms)

    return clause, args


def get_by_prefix(db_schema, table_name, column_name, value_prefix):
    conn = _get_db_connection()
    cur = conn.cursor()

    sql = "select * from " + db_schema + "." + table_name + " where " + \
          column_name + " like " + "'" + value_prefix + "%'"
    print("SQL Statement = " + cur.mogrify(sql, None))

    res = cur.execute(sql)
    res = cur.fetchall()
    conn.close()
    return res



def _get_where_clause_args(template):
    terms = []
    args = []

    if template is None or template == {}:
        clause = ""
        args = None
    else:
        for k, v in template.items():
            terms.append(k + "=%s")
            args.append(v)

        clause = " where " + " and ".join(terms)

    return clause, args


def find_by_template(db_schema, table_name,
                     template, fields=None, limit=None, offset=None):
    wc, args = _get_where_clause_args(template)

    conn = _get_db_connection()
    cur = conn.cursor()

    if not fields:
        fields = "*"

    sql = "select " + fields + " from " + db_schema + "." + table_name + " " + wc + " limit "

    if limit:
        restrictions = str(offset) + ", " + str(limit)
    else:
        restrictions = str(limit)

    sql += restrictions + ";"

    print(sql)

    try:
        res = cur.execute(sql, args=args)
        res = cur.fetchall()

        conn.close()
        return res

    except Exception as e:
        raise BaseApplicationException(e.args[1])


"""
def _get_key(jto, key_columns):
    if key_columns is not None and len(key_columns) > 0:
        result = {k: jto[k] for k in key_columns}
    else:
        result = None

    return result
"""


def _insert(db_schema, table_name, row):
    conn = _get_db_connection()
    cur = conn.cursor()

    sql = "insert into " + db_schema + "." + table_name + " "

    keys = list(row.keys())
    k = ",".join(keys)
    v = ["%s"] * len(keys)
    v = ",".join(v)
    sql += "(" + k + ") " "values(" + v + ")"
    print(sql)

    try:
        result = cur.execute(sql, tuple(row.values()))
        conn.commit()
        cur.close()
        return result

    except Exception as e:
        raise BaseApplicationException(e.args[1])


def _get_new_primary_key_value(db_schema, table_name, key_columns):
    conn = _get_db_connection()
    cur = conn.cursor()

    keys = ""
    for key in key_columns:
        keys += " max({}),".format(key)
    keys = keys[:-1]

    sql = "select" + keys + " from " + db_schema + "." + table_name + ";"

    print(sql)

    cur.execute(sql)
    res = cur.fetchall()
    conn.close()
    cur.close()

    res = res[0]
    result = dict()
    for key_max in res:
        key = key_max[key_max.find("(") + 1:key_max.find(")")]
        result[key] = res[key_max] + 1

    return result


def create(db_schema, table_name, key_columns, transfer_json):
    key_json = _get_new_primary_key_value(db_schema, table_name, key_columns)
    transfer_json.update(key_json)
    result = _insert(db_schema, table_name, transfer_json)
    if result:
        return key_json


def update(db_schema, table_name, template, row):
    conn = _get_db_connection()
    cur = conn.cursor()

    set_clause, set_args = transfer_json_to_set_clause(row)
    where_clause = template_to_where_clause(template)
    sql = "update  " + db_schema + "." + table_name + " " + set_clause + " " + where_clause
    print(sql)

    result = cur.execute(sql, set_args)
    conn.commit()
    cur.close()

    return result


def delete(db_schema, table_name, template):
    conn = _get_db_connection()
    cur = conn.cursor()

    wc, args = _get_where_clause_args(template)
    q1 = "delete from " + db_schema + "." + table_name + wc + ";"
    q2 = "select row_count() as no_of_rows_deleted;"
    print(q1)
    cur.execute(q1, args)
    print(q2)
    cur.execute(q2)
    result = cur.fetchone()
    conn.commit()

    return result
