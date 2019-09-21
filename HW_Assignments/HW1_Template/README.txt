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

    - I never reuse by_template function for by_key function. For example, I don't use find_by_template to implement
    find_by_key because it's a lot more efficient to just implement them separately. For find_by_key, you can stop search
    after getting the matching row, with the assumption that primary key constraint is not violated when the first dataset is loaded.

    - Saving

    For CSVDataTable, I create an init param called autocommit (default=False)
    If set to true, it will always call save function after insert/update/delete

    - Parameter validation

Specific to RDBDataTable implementation

    - Primary key constraint

    Not implemented in the code because MySQL has already handled, I just need to make the test case that simulates
    how MySQL reacts if the constraint is violated

    -s