# This unit test uses two tables: Pitching and Schools

from src.data_service.RDBDataTable import RDBDataTable

if __name__=='__main__':
    pitching_rdb = RDBDataTable(
        'pitching',
        'lahman2019clean'
    )

    # Total rows in Pitching before any modification should be 46699
    assert pitching_rdb.get_row_count() == 46699

    # Pitching has composite primary key consisting of 4 columns, ordered this way: ['playerID', 'teamID', 'yearID', 'stint']
    assert pitching_rdb.get_primary_key_columns() == ['playerID', 'teamID', 'yearID', 'stint']

    school_rdb = RDBDataTable(
        'schools',
        'lahman2019clean'
    )

    # Schools table doesn't have any defined primary key, so the function get_primary_key_columns() should return None
    assert school_rdb.get_primary_key_columns() == None

    # NOTE: I'm not sure whether those two functions above should return a value or set the object variable
    # In this case, I decide to just return a value for both and use those values to set
    # the _key_columns and _row_count variable in the class initialization