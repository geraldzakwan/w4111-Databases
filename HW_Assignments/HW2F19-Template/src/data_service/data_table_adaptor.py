import src.data_service.RDBDataTable as RDBDataTable

# The REST application server app.py will be handling multiple requests over a long period of time.
# It is inefficient to create an instance of RDBDataTable for each request.  This is a cache of created
# instances.
_db_tables = {}

def get_rdb_table(table_name, db_name, key_columns=None, connect_info=None):
    """

    :param table_name: Name of the database table.
    :param db_name: Schema/database name.
    :param key_columns: This is a trap. Just use None.
    :param connect_info: You can specify if you have some special connection, but it is
        OK to just use the default connection.
    :return:
    """
    global _db_tables

    # We use the fully qualified table name as the key into the cache, e.g. lahman2019clean.people.
    key = db_name + "." + table_name

    # Have we already created and cache the data table?
    result = _db_tables.get(key, None)

    # We have not yet accessed this table.
    if result is None:

        # Make an RDBDataTable for this database table.
        result = RDBDataTable.RDBDataTable(table_name, db_name, key_columns, connect_info)

        # Add to the cache.
        _db_tables[key] = result

    return result


#########################################
#
#
# YOU HAVE TO IMPLEMENT THE FUNCTIONS BELOW.
#
#
# -- TO IMPLEMENT --
#########################################


def get_databases():
    """

    :return: A list of databases/schema at this endpoint.
    """

    # -- TO IMPLEMENT --
    # Design decision: Return only what's cached
    # Needs to later add all accessed tables to this _db_tables cache

    global _db_tables

    set_of_databases = set([])

    for key in _db_tables:
        database_name = key.split('.')[0]
        set_of_databases.add(database_name)

    return list(set_of_databases)


def get_tables(dbname):
    """

    :param dbname: The name of a database/schema
    :return: List of tables in the database.
    """

    # -- TO IMPLEMENT --
    # Design decision: Return only what's cached

    global _db_tables

    set_of_tables = set([])

    for key in _db_tables:
        database_name, table_name = key.split('.')

        if database_name == dbname:
            set_of_tables.add(table_name)

    return list(set_of_tables)

# Some helper functions
def is_empty(data):
    if data is None:
        return True

    if len(data) == 0:
        return True

    return False







