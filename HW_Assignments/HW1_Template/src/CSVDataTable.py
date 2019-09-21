from src.BaseDataTable import BaseDataTable
from src.Helper import Helper
import copy
import csv
import logging
import json
import os

class CSVDataTable(BaseDataTable):
    '''
    The implementation classes (XXXDataTable) for CSV database, relational, etc. with extend the
    base class and implement the abstract methods.
    '''

    _rows_to_print = 10
    _no_of_separators = 2

    def __init__(self, table_name, connect_info, key_columns, debug=True, load=True, rows=None):
        '''

        :param table_name: Logical name of the table.
        :param connect_info: Dictionary of parameters necessary to connect to the data.
        :param key_columns: List, in order, of the columns (fields) that comprise the primary key.
        '''
        self._data = {
            'table_name': table_name,
            'connect_info': connect_info,
            'key_columns': key_columns,
            'debug': debug
        }

        self._logger = logging.getLogger()

        self._logger.debug('CSVDataTable.__init__: data = ' + json.dumps(self._data, indent=2))

        if rows is not None:
            self._rows = copy.copy(rows)
        else:
            self._rows = []
            self._load()

    def __str__(self):

        result = 'CSVDataTable: config data = \n' + json.dumps(self._data, indent=2)

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
                    tmp_row[k] = '***'
                rows_to_print.append(tmp_row)

            rows_to_print.extend(self._rows[int(-1*temp_r)-1:-1])

        df = pd.DataFrame(rows_to_print)
        result += '\nSome Rows: = \n' + str(df)

        return result

    def _add_row(self, r):
        if self._rows is None:
            self._rows = []
        self._rows.append(r)

    def _delete_row(self, r_idx):
        if self._rows is not None:
            del self._rows[r_idx]

    # Remember to delete from the back so that the indexes don't mess up
    def _delete_rows(self, r_indexes):
        for idx in sorted(r_indexes, reverse=True):
            self._delete_row(idx)

    def _modify_row(self, idx, new_values):
        for key in new_values:
            self._rows[idx][key] = new_values[key]

    def _modify_rows(self, r_indexes, new_values):
        for idx in r_indexes:
            self._modify_rows(idx, new_values)

    def _load(self):

        dir_info = self._data['connect_info'].get('directory')
        file_n = self._data['connect_info'].get('file_name')
        full_name = os.path.join(dir_info, file_n)

        i = 0
        with open(full_name, 'r') as txt_file:
            csv_d_rdr = csv.DictReader(txt_file)
            for r in csv_d_rdr:
                if i == 0:
                    if not Helper.is_column_list_valid(self._data['key_columns'], r.keys()):
                        print('Specified primary keys don\'t match table columns')
                        raise Exception
                self._add_row(r)
                i = i + 1

        # Primary key checking constraint, using find_by_template to check if it breaks
        # If it is, load_fail
        self._logger.debug('CSVDataTable._load: Loaded ' + str(len(self._rows)) + ' rows')

    def get_columns(self):
        return self._rows[0].keys()

    def get_rows(self):
        return self._rows

    def save(self):
        '''
        Write the information back to a file.
        :return: None
        '''
        dir_info = self._data['connect_info'].get('directory')
        file_n = self._data['connect_info'].get('file_name')
        full_name = os.path.join(dir_info, file_n)

        try:
            with open(full_name, 'w') as txt_file:
                csv_d_wrtr = csv.DictWriter(txt_file, fieldnames=list(self.get_rows()[0].keys()))
                csv_d_wrtr.writeheader()
                for data in self.get_rows():
                    csv_d_wrtr.writerow(data)
        except IOError:
            self._logger.error('Error when saving to csv: ' + IOError)
            raise Exception

    def _extract_needed_fields(self, field_list, row):
        # If no field is specified, return all fields
        if Helper.is_empty(field_list):
            return row

        needed_fields = {}
        for key in field_list:
            needed_fields[key] = row.get(key)

        return needed_fields

    def _violate_primary_key_constraint(self, record):
        template = Helper.extract_key_columns_and_values_rom_template(record, self._data['key_columns'])

        duplicate_record = self.find_by_template(template)
        if Helper.is_empty(duplicate_record):
            return False

        return True

    def find_by_primary_key(self, key_fields, field_list=None):
        '''

        :param key_fields: The list with the values for the key_columns, in order, to use to find a record.
        :param field_list: A subset of the fields of the record to return.
        :return: None, or a dictionary containing the requested fields for the record identified
            by the key.
        '''
        if Helper.is_empty(self._data['key_columns']):
            print('Table has no primary keys')
            raise Exception

        if not Helper.are_key_fields_valid(key_fields, self._data['key_columns']):
            print('Key fields are not valid')
            raise Exception

        template = Helper.convert_key_fields_to_template(key_fields, self._data['key_columns'])
        result = self.find_by_template(template, field_list, 1)

        if Helper.is_empty(result):
            return None

        return result[0]

    def find_by_template(self, template, field_list=None, limit=None, offset=None, order_by=None):
        '''

        :param template: A dictionary of the form { 'field1' : value1, 'field2': value2, ...}
        :param field_list: A list of request fields of the form, ['fielda', 'fieldb', ...]
        :param limit: Do not worry about this for now.
        :param offset: Do not worry about this for now.
        :param order_by: Do not worry about this for now.
        :return: A list containing dictionaries. A dictionary is in the list representing each record
            that matches the template. The dictionary only contains the requested fields.
        '''
        if not Helper.is_empty(template):
            if not Helper.is_template_valid(template, self.get_columns()):
                print('Some columns in the specified template don\'t match table columns')
                raise Exception

        if not Helper.is_empty(field_list):
            if not Helper.is_column_list_valid(field_list, self.get_columns()):
                print('Some columns in the specified field_list don\'t match table columns')
                raise Exception

        matching_rows = []
        for row in self.get_rows():
            if limit is not None:
                if len(matching_rows) == limit:
                    break

            if Helper.matches_template(row, template):
                matching_rows.append(self._extract_needed_fields(field_list, row))

        return matching_rows

    def delete_by_key(self, key_fields):
        '''

        Deletes the record that matches the key.

        :param template: A template.
        :return: A count of the rows deleted.
        '''
        if Helper.is_empty(self._data['key_columns']):
            print('Table has no primary keys')
            raise Exception

        if not Helper.are_key_fields_valid(key_fields, self._data['key_columns']):
            print('Key fields are not valid')
            raise Exception

        template = Helper.convert_key_fields_to_template(key_fields, self._data['key_columns'])

        return self.delete_by_template(template, 1)

    def delete_by_template(self, template, limit=None):
        '''

        :param template: Template to determine rows to delete.
        :return: Number of rows deleted.
        '''
        if not Helper.is_empty(template):
            if not Helper.is_template_valid(template, self.get_columns()):
                print('Some columns in the specified template don\'t match table columns')
                raise Exception

        rows = self.get_rows()
        r_indexes = []
        for i in range(0, len(rows)):
            if limit is not None:
                if len(r_indexes) == limit:
                    break

            if Helper.matches_template(rows[i], template):
                r_indexes.append(i)

        if len(r_indexes) == 0:
            return 0
        elif len(r_indexes) == 1:
            self._delete_row(r_indexes[0])
            return 1
        else:
            self._delete_rows(r_indexes)
            return len(r_indexes)

    def update_by_key(self, key_fields, new_values):
        '''

        :param key_fields: List of value for the key fields.
        :param new_values: A dict of field:value to set for updated row.
        :return: Number of rows updated.
        '''
        if Helper.is_empty(self._data['key_columns']):
            print('Table has no primary keys')
            raise Exception

        if not Helper.are_key_fields_valid(key_fields, self._data['key_columns']):
            print('Key fields are not valid')
            raise Exception

        if Helper.is_empty(new_values):
            return 0

        template = Helper.convert_key_fields_to_template(key_fields, self._data['key_columns'])
        idx = -1
        i = 0
        # Iterate each row in data and update matching row if any
        for row in self.get_rows():
            if Helper.matches_template(row, template):
                idx = i
                break
            i = i + 1

        if idx == -1:
            # No matching row
            return 0
        else:
            self._modify_row(idx, new_values)
            return 1

    def update_by_template(self, template, new_values):
        '''

        :param template: Template for rows to match.
        :param new_values: New values to set for matching fields.
        :return: Number of rows updated.
        '''
        if template is None or new_values is None:
            return 0

        if len(template) == 0 or len(new_values) == 0:
            return 0

        if self._violate_primary_key_constraint(template):
            print('Violates primary key constraint')
            raise Exception

        template = Helper.convert_key_fields_to_template(key_fields, self._data['key_columns'])
        r_indexes = []
        i = 0
        for row in self.get_rows():
            if Helper.matches_template(row, template):
                r_indexes.append(i)
            i = i + 1

        # Use either _modify_row or _modify_rows function
        # and return appropriate value
        if len(r_indexes) == 0:
            return 0
        elif len(r_indexes) == 1:
            self._modify_row(r_indexes[0], new_values)
            return 1
        else:
            self._modify_rows(r_indexes, new_values)
            return len(r_indexes)

    def insert(self, new_record):
        '''

        :param new_record: A dictionary representing a row to add to the set of records.
        :return: None
        '''
        if not Helper.is_new_record_valid(new_record, self._rows[0].keys()):
            return

        if self._violate_primary_key_constraint(new_record):
            print('Violates primary key constraint')
            raise Exception

        self._add_row(new_record)

if __name__=='__main__':
    data_dir = os.path.abspath('../Data/Baseball')

    csv_data_tbl = CSVDataTable(
        'Appearances', {
            'directory': data_dir,
            'file_name': 'Appearances.csv'
        }, [
            'playerID',
            'teamID',
            'yearID'
        ]
    )

    csv_data_tbl.insert({'yearID': '2015', 'teamID': 'ABC', 'lgID': 'NL', 'playerID': 'aardsda01', 'G_all': '33', 'GS': '0', 'G_batting': '30', 'G_defense': '33', 'G_p': '33', 'G_c': '0', 'G_1b': '0', 'G_2b': '0', 'G_3b': '0', 'G_ss': '0', 'G_lf': '0', 'G_cf': '0', 'G_rf': '0', 'G_of': '0', 'G_dh': '0', 'G_ph': '0', 'G_pr': '0'})

    # print('Created table = ' + str(csv_data_tbl))

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