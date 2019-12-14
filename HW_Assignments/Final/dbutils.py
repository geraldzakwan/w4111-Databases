import pymysql
import logging
import json

default_db_host="localhost"
default_db_user="dbuser"
default_db_password="dbuserdbuser"

def get_new_connection(host=default_db_host,
                       user=default_db_user,
                       password=default_db_password):
    result_conn = pymysql.connect(
        host=host,
        user=user,
        password=password,
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=False)
    return result_conn


final_conn = get_new_connection(default_db_host, default_db_user, default_db_password)


def tx_commit(conn):
    conn.commit()


def tx_rollback(conn):
    conn.rollback()


def conn_close(conn):
    conn.close()


def run_q(sql, args=None, fetch=True, cur=None, conn=final_conn, commit=True, debug=False):
    '''
    Helper function to run an SQL statement.

    This is a modification that better supports HW1. An RDBDataTable MUST have a connection specified by
    the connection information. This means that this implementation of run_q MUST NOT try to obtain
    a defailt connection.

    :param sql: SQL template with placeholders for parameters. Cannot be NULL.
    :param args: Values to pass with statement. May be null.
    :param fetch: Execute a fetch and return data if TRUE.
    :param conn: The database connection to use.
    :param cur: The cursor to use.
    :param commit: Commit after completing transaction.
    :param debug: If true, print some debugging messages.

    :return: A pair of the form (execute response, fetched data). There will only be fetched data if
        the fetch parameter is True. 'execute response' is the return from the connection.execute, which
        is typically the number of rows effected.
    '''

    cursor_created = False
    connection_created = False

    try:

        if conn is None:
            connection_created = True
            conn = get_new_connection(default_db_host, default_db_user, default_db_password)
            cursor_created = True
            cur = conn.cursor()

        if cur is None:
            cursor_created = True
            cur = conn.cursor()

        if args is not None:
            log_message = cur.mogrify(sql, args)
        else:
            log_message = sql

        if debug:
            print("Executing SQL = " + log_message)

        res = cur.execute(sql, args)

        if fetch:
            data = cur.fetchall()
        else:
            data = None

        # Do not ask.
        if commit == True:
            conn.commit()

        if cursor_created:
            cur.close()
        if connection_created:
            conn.close()

    except Exception as e:
        if commit == True:
            conn.rollback()
        if cursor_created:
            cur.close()
        if connection_created:
            conn.close()
        raise (e)

    return (res, data)