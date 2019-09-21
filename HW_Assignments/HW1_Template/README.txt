Some general assumptions:

- If field_list = None or field_list = empty, I treated it just like "SELECT *" SQL query, so I return all fields
- If table has no primary key, these are the behaviors:
    1. find_by_primary_key returns None
    2. delete_by_primary_key returns 0
    3. update_by_primary_key returns 0
- If template is None/empty, these are the behaviors:
    1. find_by_template returns all rows -> like SELECT without WHERE clause
    2. delete_by_template deletes everything ->
    3. update_by_template updates everything ->

Assumptions for RDBDataTable:

- 'table_name' param passed on class constructor is the name of the actual table in mysql. I use this for querying stuffs.
- Key columns passed to class constructor matches . This is mentioned by Prof. Ferguson in Piazza, question 115

Some design decisions:

General:

    - For find_by_primary_key, return None if no key_field is specified
    - For insert, I raise Exception if there is any missing field

Specific to CSVDataTable implementation

    - Primary key constraint:

    Since I use the full Appearances database for testing and
    it's very heavy to do primary key checking in _add_row (too many rows to check), I do primary key checking on
    the insert function instead. This assumes that the very first loaded dataset doesn't break the primary key constraint.

    - I also do primary key checking on update

    - I implement the limit param in find_by_template so I can use that for find_by_primary_key by setting the limit to 1.
    Thus, you can stop search after getting the first matching row, with the assumption that primary key constraint
    is not violated when the first dataset is loaded. So, it is more efficient.

    - I also do the same for delete and update (using limit param so I can reuse the _by_template functions efficiently)

    - Saving

    For CSVDataTable, I create an init param called autocommit (default=False)
    If set to true, it will always call save function after insert/update/delete

    - Parameter validation

    Did params validation for template, field_list, etc. If columns don't match throws exception

    - More than one rows checking

    I use compare_two_list_of_dicts function because sometimes the elements order in the returned list of find_by_template
    can be different from the elements order in the returned list if we query using SQL

Specific to RDBDataTable implementation

    - Primary key constraint

    Not implemented in the code because MySQL has already handled, I just need to make the test case that simulates
    how MySQL reacts if the constraint is violated

    - Incomplete fields

    Using MysQL

    - Commit

    I specify a param called autocommit. If set to true, after every insert, update or delete operations, it affects
    the database directly. If set to false, you need to call function self.commit() everytime you want to apply the
    changes to the database. For rdb_table_test.py, I set autocommit to false so that for every test function, db
    condition is the same.