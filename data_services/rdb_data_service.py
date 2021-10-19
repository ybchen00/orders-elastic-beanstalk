import pymysql
import logging

import middleware.service_factory as service_factory

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()
logger.setLevel(logging.INFO)


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
            s += " AND "
        s += k + "='" + v + "'"

    if s != "":
        s = "WHERE " + s

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

        clause = " where " + " AND ".join(terms)

    return clause, args


def find_by_template(db_schema, table_name,
                     template):  # TODO: the schema and table_name should really be passed in a better way
    wc, args = _get_where_clause_args(template)

    conn = _get_db_connection()
    cur = conn.cursor()

    sql = "select * from " + db_schema + "." + table_name + " " + wc
    print(sql)
    res = cur.execute(sql, args=args)
    res = cur.fetchall()

    conn.close()

    return res


def _get_key(jto, key_columns):
    if key_columns is not None and len(key_columns) > 0:
        result = {k: jto[k] for k in key_columns}
    else:
        result = None

    return result


def run_q(self, q, args, cnx=None, cursor=None, commit=True, fetch=True):
    """

    :param q: The query string to run.
    :param fetch: True if this query produces a result and the function should perform and return fetchall()
    :return:
    """

    cursor_created = False
    cnx_created = False
    result = None

    try:
        if cnx is None:
            cnx = self._cnx
            cursor = self._cnx.cursor()
            cursor_created = True
        else:
            cnx = self._cnx
            cursor = cnx.cursor()
            cnx_created = True
            cursor_created = True

        log_message = cursor.mogrify(q, args)
        logger.debug(log_message)

        res = cursor.execute(q, args)

        if fetch:
            result = cursor.fetchall()
        else:
            result = res

        if commit:
            cnx.commit()
        if cursor_created:
            cursor.close()
        if cnx_created:
            cnx.close()
    except Exception as e:
        logger.warning("RDBDataTable.run_q, e = ", e)

        if commit:
            cnx.commit()
        if cursor_created:
            cursor.close()
        if cnx_created:
            cnx.close()

        raise e

    return result


def _insert(db_schema, table_name, row):
    conn = _get_db_connection()
    cur = conn.cursor()

    sql = "INSERT into " + db_schema + "." + table_name + " "

    keys = list(row.keys())
    k = ",".join(keys)
    v = ["%s"] * len(keys)
    v = ",".join(v)
    sql += "(" + k + ") " "values(" + v + ")"
    print(sql)

    result = cur.execute(sql, tuple(row.values()))
    conn.commit()
    cur.close()

    return result


def create(db_schema, table_name, key_columns, transfer_json):
    result = _insert(db_schema, table_name, transfer_json)
    if result:
        result = _get_key(transfer_json, key_columns)
    return result


def update(db_schema, table_name, template, row):
    conn = _get_db_connection()
    cur = conn.cursor()

    set_clause, set_args = transfer_json_to_set_clause(row)
    where_clause = template_to_where_clause(template)
    sql = "UPDATE  " + db_schema + "." + table_name + " " + set_clause + " " + where_clause
    print(sql)

    result = cur.execute(sql, set_args)
    conn.commit()
    cur.close()

    return result

