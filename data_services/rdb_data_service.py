import pymysql
import json
import logging

import middleware.service_factory as context

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def _get_db_connection():

    db_connect_info = context.get_db_info()

    logger.info("RDBService._get_db_connection:")
    logger.info("\t HOST = " + db_connect_info['host'])

    db_info = context.get_db_info()
    db_connection = pymysql.connect(
       **db_info
    )
    return db_connection

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
    clause = None

    if template is None or template == {}:
        clause = ""
        args = None
    else:
        for k, v in template.items():
            terms.append(k + "=%s")
            args.append(v)

        clause = " where " +  " AND ".join(terms)


    return clause, args

def find_by_template(db_schema, table_name, template, field_list):

    wc, args = _get_where_clause_args(template)

    conn = _get_db_connection()
    cur = conn.cursor()

    sql = "select * from " + db_schema + "." + table_name + " " + wc
    print(sql)
    res = cur.execute(sql, args=args)
    res = cur.fetchall()

    conn.close()

    return res

def _insert(self, row):
    keys = row.keys()
    q = "INSERT into " + self._table_file + " "
    s1 = list(keys)
    s1 = ",".join(s1)

    q += "(" + s1 + ") "

    v = ["%s"] * len(keys)
    v = ",".join(v)

    q += "values(" + v + ")"

    params = tuple(row.values())

    result = self.run_q(q, params, fetch=False)

    return result

def create(self, transfer_json):
    result = _insert(transfer_json)
    if result:
        result = self._get_key(transfer_json)
    return result

def update(self, template, row):
    set_clause, set_args = self.transfer_json_to_set_clause(row)
    where_clause = self.template_to_where_clause(template)
    q = "UPDATE  " + self._table_file + " " + set_clause + " " + where_clause
    result = self.run_q(q, set_args, fetch=False)

    return result
