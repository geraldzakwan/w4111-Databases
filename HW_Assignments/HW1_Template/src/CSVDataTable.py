
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

    def _delete_rows(self, r_indexes):
        for idx in sorted(r_indexes, reverse=True):
            self._delete_row(idx)

    def modify_row(self, idx, new_values):
        for key in new_values:
            self._rows[idx][key] = new_values[key]

    def modify_rows(self, r_indexes, new_values):
        for idx in idx_to_delete:
            self.modify_rows(r_indexes, new_values)

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

        # Construct filepath from connect_info
        dir_info = self._data["connect_info"].get("directory")
        file_n = self._data["connect_info"].get("file_name")
        full_name = os.path.join(dir_info, file_n)

        # Try-except block to catch any error and print it if any
        try:
            with open(full_name, "w") as txt_file:
                # Use csv.DictWriter to write to csv, fieldnames are fetched from the first row keys
                csv_d_wrtr = csv.DictWriter(txt_file, fieldnames=list(self.get_rows()[0].keys()))
                csv_d_wrtr.writeheader()
                for data in self.get_rows():
                    csv_d_wrtr.writerow(data)
        except IOError:
            self._logger.error(IOError)

    @staticmethod
    def matches_template(row, template):

        result = True
        if template is not None:
            for k, v in template.items():
                if v != row.get(k, None):
                    result = False
                    break

        return result

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

        # Directly return if param is None or empty
        if key_fields is None:
            #Q SHOULD FAIL, NO KEY -> return NONE?
            return None #correct
            # return self.get_rows()

        if len(key_fields) == 0:
            #Q SHOULD FAIL, NO KEY -> return NONE?
            return None
            # return empty dictionary, reference from TA Kiran
            # return {}
            # return self.get_rows()

        #Q Directly return if no primary key is set
        if self._data['key_columns'] is None or len(self._data['key_columns']) == 0:
            return None

        template = self.convert_to_template(key_fields)
        # Iterate each row in data and return matching row as dict if any
        ret_list = self.find_by_template(template, field_list)

        if len(ret_list) == 0:
            # No matching row
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

        # Directly return if param is None or empty
        if template is None:
            # Q SHOULD FAIL, NO KEY -> return NONE?
            return []
            # return None  # correct
            # return self.get_rows()

        if len(template) == 0:
            # Q SHOULD FAIL, NO KEY -> return NONE?
            return []
            # return None
            # return empty dictionary, reference from TA Kiran
            # return {}
            # return self.get_rows()

        # Iterate each row in data and add matching rows to a list
        matching_rows = []
        for row in self.get_rows():
            if CSVDataTable.matches_template(row, template):
                #Q What if field_list is empty? I assume appending the whole will be suitable, something like SELECT *
                if field_list is None or len(field_list) == 0:
                    matching_rows.append(row)
                else:
                    # If not, copy needed fields from the row to return variable
                    needed_fields = {}
                    for key in field_list:
                        needed_fields[key] = row.get(key)
                    matching_rows.append(needed_fields)

        return matching_rows

    def delete_by_key(self, key_fields):
        """

        Deletes the record that matches the key.

        :param template: A template.
        :return: A count of the rows deleted.
        """

        #Q Directly return if param is None or empty
        if key_fields is None:
            return 0

        if len(key_fields) == 0:
            return 0

        #Q Directly return if no primary key is set
        if self._data['key_columns'] is None or len(self._data['key_columns']) == 0:
            return 0

        template = self.convert_to_template(key_fields)
        # Iterate each row in data and add the row's index if key matches
        rows = self.get_rows()
        for i in range(0, len(rows)):
            if CSVDataTable.matches_template(rows[i], template):
                self._delete_row(i)
                #Q If we can assume there is no duplicate, we can return 1
                return 1

        # No matching row
        return 0

    def delete_by_template(self, template):
        """

        :param template: Template to determine rows to delete.
        :return: Number of rows deleted.
        """

        #Q Directly return if param is None or empty
        if template is None:
            return 0

        if len(template) == 0:
            return 0

        # Iterate each row in data and add the row's index if key matches
        rows = self.get_rows()
        r_indexes = []
        for i in range(0, len(rows)):
            if CSVDataTable.matches_template(rows[i], template):
                r_indexes.append(i)

        # Use either _delete_row or _delete_rows function
        # and return appropriate value
        if len(r_indexes) == 0:
            return 0
        elif len(r_indexes) == 1:
            self._delete_row(r_indexes[0])
            return 1
        else:
            self._delete_rows(r_indexes)
            return len(r_indexes)

    def update_by_key(self, key_fields, new_values):
        """

        :param key_fields: List of value for the key fields.
        :param new_values: A dict of field:value to set for updated row.
        :return: Number of rows updated.
        """

        #Q Directly return if param is None or empty
        if key_fields is None or new_values is None:
            return 0

        if len(key_fields) == 0 or len(new_values) == 0:
            return 0

        #Q Directly return if no primary key is set
        if self._data['key_columns'] is None or len(self._data['key_columns']) == 0:
            return 0

        template = self.convert_to_template(key_fields)
        idx = -1
        i = 0
        # Iterate each row in data and update matching row if any
        for row in self.get_rows():
            if CSVDataTable.matches_template(row, template):
                idx = i
                break
            i = i + 1

        if idx == -1:
            # No matching row
            return 0
        else:
            self.modify_row(idx, new_values)
            return 1

    def update_by_template(self, template, new_values):
        """

        :param template: Template for rows to match.
        :param new_values: New values to set for matching fields.
        :return: Number of rows updated.
        """

        #Q Directly return if param is None or empty
        if template is None or new_values is None:
            return 0

        if len(template) == 0 or len(new_values) == 0:
            return 0

        template = self.convert_to_template(key_fields)
        r_indexes = []
        i = 0
        # Iterate each row in data and update matching row if any
        for row in self.get_rows():
            if CSVDataTable.matches_template(row, template):
                r_indexes.append(i)
            i = i + 1

        # Use either _modify_row or _modify_rows function
        # and return appropriate value
        if len(r_indexes) == 0:
            return 0
        elif len(r_indexes) == 1:
            self.modify_row(r_indexes[0], new_values)
            return 1
        else:
            self.modify_rows(r_indexes, new_values)
            return len(r_indexes)

    def insert(self, new_record):
        """

        :param new_record: A dictionary representing a row to add to the set of records.
        :return: None
        """

        # Directly return if new_record is not None or
        if new_record is None or len(new_record) == 0:
            return

        # Get all the primary key values from new_record
        key_values = convert_to_template(self, new_record)

        # Check if there is a record with the same set of primary keys
        duplicate_record = self.find_by_primary_key(key_values)

        # Q NEED TO RAISE AN EXCEPTION IF THERE IS DUPLICATE PRIMARY KEY
        if duplicate_record is not None and len(duplicate_record) > 0:
            # What kind of exception? Does general exception suffice?
            raise Exception

        self._add_row(new_record)

    def get_rows(self):
        return self._rows

if __name__=='__main__':
    data_dir = os.path.abspath("../Data/Baseball")

    connect_info = {
        "directory": data_dir,
        "file_name": "People.csv"
    }

    csv_data_tbl = CSVDataTable("people", connect_info, ["playerID"])
    print("Created table = " + str(csv_data_tbl))

    # List of questions for TA
    # - I know that this has been discussed several times, but just to make sure that all all attributes are treated as string or plain text right?
    # - Confusion for by_key methods, do we accept list of key values or template? I've read Piazza and come to conclusion that it should be list of key values. Just need to reconfirm.
    # - Can we assume no two rows have the same set of primary keys so that for delete_by_key and update_by_key we always return either 0 or 1?
    # - Corner cases? e.g. key_fields, field_list, template or new_values is None or empty. Do we assume ourselves?
    # e.g. if key_fields or template is empty for find function, we return all rows (like select all)
    # - How to test save function? -> no need
    # - How to construct unit test -> Any specified format -> seems that this is loose
    # https://piazza.com/class/jy3jm0i73f8584?cid=71
    # - Prof. mentions that we should use Batting, People and Appearance data as toy database. Should we create unit test on all those data?
    # - Should I cover all cases? Can I use pytest? It makes me easier to track test coverage