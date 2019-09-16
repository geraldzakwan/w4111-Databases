
from src.BaseDataTable import BaseDataTable
import copy
import csv
import logging
import json
import os
import pandas as pd

pd.set_option("display.width", 256)
pd.set_option('display.max_columns', 20)

class CSVDataTable(BaseDataTable):
    """
    The implementation classes (XXXDataTable) for CSV database, relational, etc. with extend the
    base class and implement the abstract methods.
    """

    _rows_to_print = 10
    _no_of_separators = 2

    def __init__(self, table_name, connect_info, key_columns, debug=True, load=True, rows=None):
        """

        :param table_name: Logical name of the table.
        :param connect_info: Dictionary of parameters necessary to connect to the data.
        :param key_columns: List, in order, of the columns (fields) that comprise the primary key.
        """
        self._data = {
            "table_name": table_name,
            "connect_info": connect_info,
            "key_columns": key_columns,
            "debug": debug
        }

        self._logger = logging.getLogger()

        self._logger.debug("CSVDataTable.__init__: data = " + json.dumps(self._data, indent=2))

        if rows is not None:
            self._rows = copy.copy(rows)
        else:
            self._rows = []
            self._load()

    def __str__(self):

        result = "CSVDataTable: config data = \n" + json.dumps(self._data, indent=2)

        no_rows = len(self._rows)
        if no_rows <= CSVDataTable._rows_to_print:
            rows_to_print = self._rows[0:no_rows]
        else:
            temp_r = int(CSVDataTable._rows_to_print / 2)
            rows_to_print = self._rows[0:temp_r]
            keys = self._rows[0].keys()

            for i in range(0,CSVDataTable._no_of_separators):
                tmp_row = {}
                for k in keys:
                    tmp_row[k] = "***"
                rows_to_print.append(tmp_row)

            rows_to_print.extend(self._rows[int(-1*temp_r)-1:-1])

        df = pd.DataFrame(rows_to_print)
        result += "\nSome Rows: = \n" + str(df)

        return result

    def _add_row(self, r):
        if self._rows is None:
            self._rows = []
        self._rows.append(r)

    def _delete_row(self, r_idx):
        if self._rows is not None:
            del self._rows[r_idx]

    def _delete_rows(self, r_idx_list):
        for idx in r_idx_list:
            self._delete_row(idx)

    def _load(self):

        dir_info = self._data["connect_info"].get("directory")
        file_n = self._data["connect_info"].get("file_name")
        full_name = os.path.join(dir_info, file_n)

        with open(full_name, "r") as txt_file:
            csv_d_rdr = csv.DictReader(txt_file)
            for r in csv_d_rdr:
                self._add_row(r)

        self._logger.debug("CSVDataTable._load: Loaded " + str(len(self._rows)) + " rows")

    def save(self):
        """
        Write the information back to a file.
        :return: None
        """

    @staticmethod
    def matches_template(row, template):

        result = True
        if template is not None:
            for k, v in template.items():
                if v != row.get(k, None):
                    result = False
                    break

        return result

    def find_by_primary_key(self, key_fields, field_list=None):
        """

        :param key_fields: The list with the values for the key_columns, in order, to use to find a record.
        :param field_list: A subset of the fields of the record to return.
        :return: None, or a dictionary containing the requested fields for the record identified
            by the key.
        """

        # Build a template which contains key columns and their value
        template = {}
        for i in range(0, len(self._data['key_columns'])):
            template[self._data['key_columns'][i]] = key_fields[i]

        # Iterate each row in data and return matching row as dict if any
        for row in self.get_rows():
            if CSVDataTable.matches_template(row, template):
                # What if field_list is empty? I assume returning empty dict will be suitable because
                # it differs from returning None and indicates that there is actually matching row
                if field_list is None:
                    return {}

                # If not, copy needed fields from the row to return variable
                needed_fields = {}
                for key in field_list:
                    needed_fields[key] = row.get(key)
                return needed_fields

        # No matching row
        return None

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

        # Iterate each row in data and add matching rows to a list
        matching_rows = []
        for row in self.get_rows():
            if CSVDataTable.matches_template(row, template):
                # What if field_list is empty? I assume appending empty dict will be suitable because
                # it can indicate how many rows match the template
                if field_list is None:
                    matching_rows.append({})
                else:
                    # If not, copy needed fields from the row to return variable
                    needed_fields = {}
                    for key in field_list:
                        needed_fields[key] = row.get(key)
                    matching_rows.append(needed_fields)

        if len(matching_rows) > 0:
            # Will be used later to handle limit, offset and order_by
            pass

        return matching_rows

    def delete_by_key(self, key_fields):
        """

        Deletes the record that matches the key.

        :param template: A template.
        :return: A count of the rows deleted.
        """
        pass

    def delete_by_template(self, template):
        """

        :param template: Template to determine rows to delete.
        :return: Number of rows deleted.
        """
        pass

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
        return self._rows

if __name__=='__main__':
    data_dir = os.path.abspath("../Data/Baseball")

    connect_info = {
        "directory": data_dir,
        "file_name": "People.csv"
    }

    csv_data_tbl = CSVDataTable("people", connect_info, ["playerID"])
    # print("Created table = " + str(csv_data_tbl))

    # Unit test for find_by_primary_key
    # print(csv_data_tbl.find_by_primary_key(["aardsda01"], ["birthYear", "birthMonth"]))
    # print(csv_data_tbl.find_by_primary_key(["aardsda01"]))
    # print(csv_data_tbl.find_by_primary_key(["aardsda07"]))

    # Unit test for find_by_template
    # print(csv_data_tbl.find_by_template({"birthYear": "1985"}, ["birthYear", "birthMonth"]))
    # print(csv_data_tbl.find_by_template({"birthYear": "1985", "birthMonth": "5"}, ["birthYear", "birthMonth"]))
    # print(len(csv_data_tbl.find_by_template({"birthYear": "1985"}, ["birthYear", "birthMonth"])))
    # print(csv_data_tbl.find_by_template({"birthYear": "1985"}))
    # print(len(csv_data_tbl.find_by_template({"birthYear": "1985"})))
    # print(csv_data_tbl.find_by_template({"birthYear": "1585"}, ["birthYear", "birthMonth"]))

    # List of questions for TA
    # 1. How to construct unit test -> Any specified format
    # https://piazza.com/class/jy3jm0i73f8584?cid=71
    # 2. Do we treat each column differently or can it be just text for all?
    # 3. For delete_by_key why do we need to return count of rows deleted? Should that always be one?
    # Assuming no two rows have the same set of primary keys
