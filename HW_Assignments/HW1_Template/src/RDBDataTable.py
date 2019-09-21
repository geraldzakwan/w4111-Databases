from src.BaseDataTable import BaseDataTable
from src.Helper import Helper
import pymysql

class RDBDataTable(BaseDataTable):

    '''
    The implementation classes (XXXDataTable) for CSV database, relational, etc. with extend the
    base class and implement the abstract methods.
    '''

    def __init__(self, table_name, connect_info, key_columns, auto_commit=False):
        '''

        :param table_name: Logical name of the table.
        :param connect_info: Dictionary of parameters necessary to connect to the data.
        :param key_columns: List, in order, of the columns (fields) that comprise the primary key.
        '''
        self._data = {
            'table_name': table_name,
            'connect_info': connect_info,
            'key_columns': key_columns
        }

        self._connection = pymysql.connect(host=connect_info['host'], user=connect_info['user'], password=connect_info['password'], db=connect_info['db'], charset='utf8', cursorclass=pymysql.cursors.DictCursor)

        if auto_commit is not None:
            self._auto_commit = auto_commit

    def get_columns(self):
        query = 'SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = ' + '\'' + self._data['table_name'] + '\';'
        with self._connection.cursor() as cursor:
            cursor.execute(query)
            result = cursor.fetchall()

        list_of_columns = []
        for elem in result:
            list_of_columns.append(elem['COLUMN_NAME'])

        return list_of_columns

    def get_rows(self):
        return self.find_by_template({}, [])

    def commit(self):
        self._connection.commit()

    def get_connection(self):
        return self._connection

    def close_connection(self):
        if self._connection.open:
            self._connection.close()

    def _compose_template_string(self, template):
        template_string = ''
        i = 0
        for key in template:
            template_string = template_string + '`' + key + '`=\'' + template[key] + '\''
            if i < len(template) - 1:
                template_string = template_string + ' AND '
            i = i + 1

        return template_string

    def _compose_field_list_string(self, field_list):
        field_list_string = ''
        for i in range(0, len(field_list)):
            field_list_string = field_list_string + '`' + field_list[i] + '`'
            if i < len(field_list) - 1:
                field_list_string = field_list_string + ', '

        return field_list_string

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
        ret_list = self.find_by_template(template, field_list)

        if len(ret_list) == 0:
            return None
        return ret_list[0]

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
        template_string = ''
        if not Helper.is_empty(template):
            if not Helper.is_template_valid(template, self.get_columns()):
                print('Some columns in the specified template don\'t match table columns')
                raise Exception
            template_string = 'WHERE ' + self._compose_template_string(template)

        if Helper.is_empty(field_list):
            field_list_string = '*'
        else:
            field_list_string = self._compose_field_list_string(field_list)

        query = 'SELECT ' + field_list_string + ' FROM ' + '`' + self._data['table_name'] + '`' + template_string + ';'
        with self._connection.cursor() as cursor:
            try:
                cursor.execute(query)
                result = cursor.fetchall()

                if len(result) == 0:
                    return []
                return result
            except pymysql.Error as error:
                print('Failed to find record(s) in the table {}'.format(error))
                raise Exception

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
        return self.delete_by_template(template)

    def delete_by_template(self, template):
        '''

        :param template: Template to determine rows to delete.
        :return: Number of rows deleted.
        '''
        template_string = ''
        if not Helper.is_empty(template):
            if not Helper.is_template_valid(template, self.get_columns()):
                print('Some columns in the specified template don\'t match table columns')
                raise Exception
            template_string = 'WHERE ' + self._compose_template_string(template)

        query = 'DELETE FROM ' + '`' + self._data['table_name'] + '` ' + template_string
        with self._connection.cursor() as cursor:
            try:
                cursor.execute(query)
                if cursor.rowcount > 0:
                    if self._auto_commit:
                        self.commit()
                return cursor.rowcount
            except pymysql.Error as error:
                print('Failed to delete record(s) in the table {}'.format(error))
                raise Exception

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
        return self.update_by_template(template, new_values)

    def update_by_template(self, template, new_values):
        '''

        :param template: Template for rows to match.
        :param new_values: New values to set for matching fields.
        :return: Number of rows updated.
        '''
        if Helper.is_empty(new_values):
            return 0

        template_string = ''
        if not Helper.is_empty(template):
            if not Helper.is_template_valid(template, self.get_columns()):
                print('Some columns in the specified template don\'t match table columns')
                raise Exception
            template_string = 'WHERE ' + self._compose_template_string(template)

        update_string = ''
        for key in new_values:
            update_string = update_string + '`' + key + '`=\'' + new_values[key] + '\', '

        update_string = update_string[:-2]
        query = 'UPDATE ' + '`' + self._data['table_name'] + '` SET ' + update_string + template_string + ';'
        with self._connection.cursor() as cursor:
            try:
                cursor.execute(query)
                if cursor.rowcount > 0:
                    if self._auto_commit:
                        self.commit()
                return cursor.rowcount
            except pymysql.Error as error:
                print('Failed to update record(s) in the table {}'.format(error))
                raise Exception

    def insert(self, new_record):
        '''

        :param new_record: A dictionary representing a row to add to the set of records.
        :return: None
        '''
        if not Helper.is_new_record_valid(new_record, self.get_columns()):
            print('new_record must contains all columns')
            raise Exception

        column_strings = '('
        value_strings = '('
        for key in new_record:
            column_strings = column_strings + '`' + key + '`' + ', '
            value_strings = value_strings + '\'' + new_record[key] + '\'' + ', '

        column_strings = column_strings[:-2] + ')'
        value_strings = value_strings[:-2] + ')'

        with self._connection.cursor() as cursor:
            query = 'INSERT INTO ' + '`' + self._data['table_name'] + '` ' + column_strings + ' VALUES ' + value_strings + ';'
            try:
                cursor.execute(query)
                if cursor.rowcount > 0:
                    if self._auto_commit:
                        self.commit()
            except pymysql.Error as error:
                print('Failed to insert record(s) into the table {}'.format(error))
                raise Exception

if __name__=='__main__':
    appearances_rdb_data_tbl = RDBDataTable(
        'appearances', {
            'host': 'localhost',
            'user': 'dbuser',
            'password': 'dbuserdbuser',
            'db': 'lahman2019'
        }, [
           'playerID',
           'teamID',
           'yearID'
        ]
    )

    # print(type(appearances_rdb_data_tbl.find_by_primary_key(['aardsda01', 'ATL', '2015'], ['playerID', 'G_all', 'GS', 'G_batting', 'G_defense'])))
    # print(appearances_rdb_data_tbl.find_by_primary_key(['aardsda01', 'ATL', '2015'], []))

    # print(type(appearances_rdb_data_tbl.find_by_template({'G_all': '150', 'GS': '150'}, ['playerID', 'yearID'])[0]))
    # print(type(appearances_rdb_data_tbl.find_by_template({'G_all': '150', 'GS': '150'}, ['playerID', 'yearID'])))
    # print(type(appearances_rdb_data_tbl.find_by_template({'G_all': '123', 'GS': '250'}, ['playerID', 'yearID'])))

    # print(appearances_rdb_data_tbl.find_by_template({'G_all': '150', 'GS': '140', 'G_ph': '7'}))
    # print(appearances_rdb_data_tbl.find_by_template({'G_all': '150', 'GS': '140', 'G_ph': '7'}))

    # print(appearances_rdb_data_tbl.find_by_template(
    #     {'yearID': '2015', 'teamID': 'ALT', 'lgID': 'NL', 'playerID': 'aardsda01', 'G_all': '33', 'GS': '0',
    #      'G_batting': '30', 'G_defense': '33', 'G_p': '33', 'G_c': '0', 'G_1b': '0', 'G_2b': '0', 'G_3b': '0',
    #      'G_ss': '0', 'G_lf': '0', 'G_cf': '0', 'G_rf': '0', 'G_of': '0', 'G_dh': '0', 'G_ph': '0', 'G_pr': '0'}, {}))
    # appearances_rdb_data_tbl.insert({'yearID': '2015', 'teamID': 'ALT', 'lgID': 'NL', 'playerID': 'aardsda01', 'G_all': '33', 'GS': '0', 'G_batting': '30', 'G_defense': '33', 'G_p': '33', 'G_c': '0', 'G_1b': '0', 'G_2b': '0', 'G_3b': '0', 'G_ss': '0', 'G_lf': '0', 'G_cf': '0', 'G_rf': '0', 'G_of': '0', 'G_dh': '0', 'G_ph': '0', 'G_pr': '0'})
    # print(appearances_rdb_data_tbl.find_by_template({'yearID': '2015', 'teamID': 'ALT', 'lgID': 'NL', 'playerID': 'aardsda01', 'G_all': '33', 'GS': '0', 'G_batting': '30', 'G_defense': '33', 'G_p': '33', 'G_c': '0', 'G_1b': '0', 'G_2b': '0', 'G_3b': '0', 'G_ss': '0', 'G_lf': '0', 'G_cf': '0', 'G_rf': '0', 'G_of': '0', 'G_dh': '0', 'G_ph': '0', 'G_pr': '0'}, {}))

    # print(appearances_rdb_data_tbl.delete_by_key(['aardsda01', 'ATL', '2015']))
    # print(appearances_rdb_data_tbl.delete_by_key(['aardsda01', 'ATL', '2015']))
    # print(appearances_rdb_data_tbl.get_columns())

    appearances_rdb_data_tbl.delete_by_key(["aardsda01", "ABC", "2015"])
    # Insert a dummy record
    appearances_rdb_data_tbl.insert(
        {'yearID': '2015', 'teamID': 'ABC', 'lgID': 'NL', 'playerID': 'aardsda01', 'G_all': '33', 'GS': '0',
         'G_batting': '30', 'G_defense': '33', 'G_p': '33', 'G_c': '0', 'G_1b': '0', 'G_2b': '0', 'G_3b': '0',
         'G_ss': '0', 'G_lf': '0', 'G_cf': '0', 'G_rf': '0', 'G_of': '0', 'G_dh': '0', 'G_ph': '0',
         'G_pr': '0'})
    # Delete succeeds
    appearances_rdb_data_tbl.delete_by_key(["aardsda01", "ABC", "2015"])