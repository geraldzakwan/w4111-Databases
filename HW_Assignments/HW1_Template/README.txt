This README consists of three parts:

1. Setup
2. Assumptions
3. Design Decisions/Implementations

1. Setup
    a. Install requirement: pip install -r requirements.txt
        - I don't use pandas library but since it is included in started code for CSVDataTable, I keep it as is
    b. If using PyCharm, set root to the folder above src and tests
    c. To run unit_test:
        - pytest tests/csv_table_test.py --cov=src.CSVDataTable --cov-report term-missing
        - pytest tests/rdb_table_test.py --cov=src.RDBDataTable --cov-report term-missing
    d. Detail on test:
        - I have confirmed with Kiran (one of the TAs) that I can use pytest library.
        - I use the Appearances.csv and Appearances table in MySQL for the test cases.
        - Primary keys are `playerID`, `teamID` and `yearID` as Prof. Ferguson explained in the class.
        - I don't use unit_tests.py as several TAs in Piazza state that it isn't necessary.

2. Assumptions
    a. Some general assumptions (apply for CSV & RDB data table):
        1. If field_list is None or is an empty list, I treat it like "SELECT *" in an SQL query. Thus, I return all fields.
          https://piazza.com/class/jy3jm0i73f8584?cid=103
        2. If a table has no primary key, all by_key functions will raise an Exception if called.
          https://piazza.com/class/jy3jm0i73f8584?cid=50
        3. If key_field is None or is an empty list, all by_key functions will raise an Exception.
        4. If template is None or is an empty dictionary, these are the behaviors:
            - find_by_template returns all rows -> like SELECT without WHERE clause
            - delete_by_template deletes all rows -> like DELETE without WHERE clause
            - update_by_template updates all rows -> like UPDATE without WHERE clause
          https://piazza.com/class/jy3jm0i73f8584?cid=189

    b. Assumptions for RDBDataTable:
        1. 'table_name' param passed to the class constructor is the name of the actual table in MySQL.
        I use this 'table_name' value to build every query.
        2. We don't need to set primary key through code, instead we set it in the MySQL or in the Workbench.
        https://piazza.com/class/jy3jm0i73f8584?cid=245
        3. Key columns passed to the class constructor will always match the actual primary keys in MySQL.
        https://piazza.com/class/jy3jm0i73f8584?cid=115

    c. Assumptions for CSVDataTable:
        1. 'key_columns' param pass to the class constructor can sometime be inappropriate, e.g. it can create duplicate primary keys
        https://piazza.com/class/jy3jm0i73f8584?cid=115

3. Design Decisions/Implementations:
    a. Design decisions/implementations for RDBDataTable
        1. Primary key constraint
            - Not implemented in the code because MySQL has already handled it, I just make the test cases that simulate
            primary key constraint violation and then catch the Exception.

        2. Parameter validation
            - Let MySQL handles and just catch the Exception (same as above). Some of the examples:
            a. Incorrectly specified columns (e.g. random column like 'test' which doesn't exist in Appearances table)
            b. Incomplete fields when trying to insert

        3. Operation commit
            - I specify a param called auto_commit in the class initialization.
            - If set to True, after every insert, update or delete operations, it will affect the database directly.
            This is the default value.
            - If set to False, you need to call function self.commit() every time you want to apply the
            changes to the database.
            - For rdb_table_test.py, I set auto_commit to false so that for every test function,
            it starts with the same database condition.

        4. Closing connection
            - Because I always create new instance for every test function, I close the connection every time
            a test function finishes by calling self._connection.close().

    b. Design decisions/implementations for CSVDataTable
        1. Primary key constraint
            - Since I use the full Appearances database for testing and it's very heavy to do primary key checking in _add_row (too many rows to check),
            I do the constraint checking on the insert function instead.
            - I also do primary key constraint checking on the update functions.
            - When data is loaded for the first time, I check if the specified primary keys will break the constraint
            through _check_primary_key_constraint_for_first_load function.
            - That function basically get the keys from all rows and check if they are unique by comparing them to their set.

        2. Limit param
            - I implement the limit param in find_by_template so I can reuse that function to
             implement find_by_primary_key function by setting the limit to 1.
            - Thus, you can stop the search after getting the first matching row, since primary key is unique.
            - I also do the same for delete and update (using limit param so I can reuse the _by_template functions efficiently)

        3. Parameter validation
            - Implement params checking and throws Exception if params are not valid. Some of the examples:
            a. Incorrectly specified columns (e.g. random column like 'test' which doesn't exist in Appearances table)
            b. Incomplete fields when trying to insert
            c. key_fields length are not the same as key_columns length

        4. Checking if find_by_template returns correct rows
            - The rows order that we get after querying using MySQL is arbitrary. Meanwhile, in CSVDataTable, I always
            return rows based on the order they appear in the CSV file.
            - Hence, in csv_table_test.py, I use compare_two_list_of_dicts function that return True if two list of
             dicts contain the same elements regardless the order.
            - Thus, I can compare my CSVDataTable find_by_template results to the actual results that are queried
            directly from MySQL. I use direct query results as the labels for all my tests.

        5. Saving back to CSV
            - I implement the save method but it has to be manually called. It is not called every time
            insert/update/delete operation is performed.

    c. Some tricky implementations that I find interesting to write:
        1. To delete more than one row, I create a list of indexes to be deleted by matching the template.
        Then, I delete it one by one from the back, e.g. row with highest index first, so that I don't mess up the indexes.

        2. To enforce unique primary key constraint when updating more than one rows, I need to be careful with param
        passing in Python. I initially use rows[i] and pass it to change_key function which basically return a modified
        version of the row based on new_values and check if there is any other row with the same primary keys.
        I then realize that it ends up updating the row itself which will always break primary key constraint
        as more and more rows are modified with the same primary keys. To solve that, I use copy.copy(rows[i])
        as the param to be passed so that the original rows[i] remains intact.
