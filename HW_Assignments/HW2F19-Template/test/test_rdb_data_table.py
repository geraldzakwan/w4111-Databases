from src.data_service.RDBDataTable import RDBDataTable

if __name__=='__main__':
    appearance_rdb = RDBDataTable(
        'appearances',
        'lahman2019clean'
    )

    assert appearance_rdb.get_row_count() == 105793

    assert appearance_rdb.get_primary_key_columns() == ['playerID', 'teamID', 'yearID']

    school_rdb = RDBDataTable(
        'schools',
        'lahman2019clean'
    )

    assert school_rdb.get_primary_key_columns() == None