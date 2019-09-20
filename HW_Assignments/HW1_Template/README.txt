Some general assumptions:

- If field_list = None or field_list = empty, I treated it just like "SELECT *" SQL query, so I return all fields

Some design decisions:

CSVDataTable implementation

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