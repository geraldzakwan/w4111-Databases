# from W4111_F19_HW1.src.BaseDataTable import BaseDataTable
from src.BaseDataTable import BaseDataTable
import pymysql

class RDBDataTable(BaseDataTable):

    """
    The implementation classes (XXXDataTable) for CSV database, relational, etc. with extend the
    base class and implement the abstract methods.
    """

    def __init__(self, table_name, connect_info, key_columns):
        """

        :param table_name: Logical name of the table.
        :param connect_info: Dictionary of parameters necessary to connect to the data.
        :param key_columns: List, in order, of the columns (fields) that comprise the primary key.
        """

        #Q Can we assume that the actual table name is the same as the logical table name -> this is for querying
        self._data = {
            "table_name": table_name,
            "connect_info": connect_info,
            "key_columns": key_columns,
        }

        self._db_client = pymysql.connect(host=connect_info["host"], user=connect_info["user"], password=connect_info["password"],
                                    db=connect_info["db"], charset='utf8', cursorclass=pymysql.cursors.DictCursor)

        self._set_primary_keys()

    def _set_primary_keys(self):
        #Q Assumption: Every key matches a specific column in the table and the type of the key is acceptable by MySQL
        # Note that TEXT type is not an acceptable type for primary key
        # In my Appearances table, I change primary key type from TEXT to VARCHAR(n) -> n depends on key length
        # e.g. ALTER TABLE `appearances` MODIFY COLUMN `playerID` VARCHAR (9) NOT NULL; -> max for playerID is 9
        # e.g. ALTER TABLE `appearances` MODIFY COLUMN `teamID` VARCHAR (3) NOT NULL; -> all teamID is of length 3
        # e.g. ALTER TABLE `appearances` MODIFY COLUMN `yearID` VARCHAR (4) NOT NULL; -> all yearID is of length 4
        with self._db_client.cursor() as cursor:
            primary_key_string = self.compose_field_list_string(self._data["key_columns"])
            # Q How if there is already a primary key? -> drop first
            # Q But then again, how to know the primary keys to drop?
            try:
                query = "ALTER TABLE " + "`" + self._data["table_name"] + "` ADD PRIMARY KEY " + "(" + primary_key_string + ");"
                cursor.execute(query)
            except:
                query = "ALTER TABLE " + "`" + self._data["table_name"] + "` DROP PRIMARY KEY, " + "ADD PRIMARY KEY " + "(" + primary_key_string + ");"
                cursor.execute(query)

    def compose_keys_string(self, key_fields):
        keys_string = ""
        for i in range(0, len(self._data["key_columns"])):
            keys_string = keys_string + "`" + self._data["key_columns"][i] + "`=\"" + key_fields[i] + "\""
            if i < len(self._data['key_columns']) - 1:
                keys_string = keys_string + " AND "

        return keys_string

    def compose_template_string(self, template):
        template_string = ""
        i = 0
        for key in template:
            template_string = template_string + "`" + key + "`=\"" + template[key] + "\""
            if i < len(template) - 1:
                template_string = template_string + " AND "
            i = i + 1

        return template_string

    def compose_field_list_string(self, field_list):
        field_list_string = ""
        for i in range(0, len(field_list)):
            field_list_string = field_list_string + "`" + field_list[i] + "`"
            if i < len(field_list) - 1:
                field_list_string = field_list_string + ", "

        return field_list_string

    # Build a template which contains key columns and their value
    def convert_to_template(self, key_fields):
        template = {}
        for i in range(0, len(self._data['key_columns'])):
            template[self._data['key_columns'][i]] = key_fields[i]

        return template

    def find_by_primary_key(self, key_fields, field_list=None):
        """

        :param key_fields: The list with the values for the key_columns, in order, to use to find a record.
        :param field_list: A subset of the fields of the record to return.
        :return: None, or a dictionary containing the requested fields for the record identified
            by the key.
        """

        if key_fields is None:
            return None
        if len(key_fields) == 0:
            return None

        template = self.convert_to_template(key_fields)
        ret_list = self.find_by_template(template, field_list)

        if len(ret_list) == 0:
            return None
        else:
            return ret_list[0]

    def find_by_template(self, template, field_list=None, limit=None, offset=None, order_by=None):
        """

        :param template: A dictionary of the form { "field1" : value1, "field2": value2, ...}
        :param field_list: A list of request fields of the form, ['fielda', 'fieldb', ...]
        :param limit: Do not worry about this for now.
        :param offset: Do not worry about this for now.
        :param order_by: Do not worry about this for now.
        :return: A list containing dictionaries. A dictionary is in the list representing each record
            that matches the template. The dictionary only contains the requested fields.
        """
        if template is None:
            return []
        if len(template) == 0:
            return []

        template_string = self.compose_template_string(template)

        # Q How if field_list is None, {} or {"*"}
        if field_list is None:
            field_list_string = "*"
        elif len(field_list) == 0:
            field_list_string = "*"
        else:
            field_list_string = self.compose_field_list_string(field_list)

        with self._db_client.cursor() as cursor:
            query = "SELECT " + field_list_string + "FROM " + "`" + self._data["table_name"] + "`" + "WHERE " + template_string + ";"
            cursor.execute(query)
            result = cursor.fetchall()

            if len(result) == 0:
                return []
            return result

    def delete_by_key(self, key_fields):
        """

        Deletes the record that matches the key.

        :param template: A template.
        :return: A count of the rows deleted.
        """
        if key_fields is None:
            return None

        if len(key_fields) == 0:
            return {}

        keys_string = self.compose_keys_string(key_fields)
        rows_returned = 0
        try:
            with self._db_client.cursor() as cursor:
                query = "DELETE " + "FROM " + "`" + self._data["table_name"] + "`" + "WHERE " + keys_string
                if cursor.rowcount > 0:
                    # commit
                    rows_returned = cursor.rowcount
        finally:
            # This will always close client connection
            # If anything fails, it will always return 0
            self._db_client.close()
            return rows_returned

    def delete_by_template(self, template):
        """

        :param template: Template to determine rows to delete.
        :return: Number of rows deleted.
        """
        if template is None:
            return None

        if len(template) == 0:
            return {}

        template_string = self.compose_template_string(template)
        rows_returned = 0
        try:
            with self._db_client.cursor() as cursor:
                query = "DELETE " + "FROM " + "`" + self._data["table_name"] + "`" + "WHERE " + template_string
                cursor.execute(query)
                if cursor.rowcount > 0:
                    # commit
                    rows_returned = cursor.rowcount
        finally:
            # This will always close client connection
            # If anything fails, it will always return 0
            self._db_client.close()
            return rows_returned

    def update_by_key(self, key_fields, new_values):
        """

        :param key_fields: List of value for the key fields.
        :param new_values: A dict of field:value to set for updated row.
        :return: Number of rows updated.
        """

    def update_by_template(self, template, new_values):
        """

        :param template: Template for rows to match.
        :param new_values: New values to set for matching fields.
        :return: Number of rows updated.
        """
        pass

    def insert(self, new_record):
        """

        :param new_record: A dictionary representing a row to add to the set of records.
        :return: None
        """
        pass

    def get_rows(self):
        #Q Can we replace this with something like select all?
        # Do we actually need to store _rows variable as class attribute?
        return self._rows

if __name__=='__main__':
    appearances_rdb_data_tbl = RDBDataTable(
        "appearances", {
            "host": "localhost",
            "user": "dbuser",
            "password": "dbuser",
            "db": "lahman2019"
        }, [
           "playerID",
           "teamID",
           "yearID"
        ]
    )

    # print(type(appearances_rdb_data_tbl.find_by_primary_key(["aardsda01", "ATL", "2015"], ["playerID", "G_all", "GS", "G_batting", "G_defense"])))
    # print(type(appearances_rdb_data_tbl.find_by_template({"G_all": "150", "GS": "150"}, ["playerID", "yearID"])[0]))
    # print(type(appearances_rdb_data_tbl.find_by_template({"G_all": "150", "GS": "150"}, ["playerID", "yearID"])))
    # print(type(appearances_rdb_data_tbl.find_by_template({"G_all": "123", "GS": "250"}, ["playerID", "yearID"])))

    print(appearances_rdb_data_tbl.find_by_template({"G_all": "150", "GS": "140", "G_ph": "7"}))