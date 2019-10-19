from src.data_service.RDBDataTable import RDBDataTable
import pytest

# Please remember to print out json format test result entries in the future for better readability.
@pytest.fixture
def appearances_rdb():
    return RDBDataTable(
        'appearances',
        'lahman2019clean',
        connect_info={
            'host': 'localhost',
            'user': 'dbuser',
            'password': 'dbuserdbuser',
            'db': 'lahman2019clean',
            'port': 3306
        }
    )

def test_get_row_count(appearances_rdb):
    assert appearances_rdb.get_row_count() == 105793

def test_get_primary_key_columns(appearances_rdb):
    assert appearances_rdb.get_primary_key_columns() == ['playerID', 'teamID', 'yearID']

