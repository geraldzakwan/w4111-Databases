from src.CSVDataTable import CSVDataTable
from src.Helper import Helper
import os
import pytest

data_dir = os.path.abspath('Data/Baseball')

@pytest.fixture
def appearances_csv():
    return CSVDataTable(
        "Appearances", {
            "directory": data_dir,
            "file_name": "Appearances.csv"
        }, [
            "playerID",
            "teamID",
            "yearID"
        ]
    )

def test_find_by_primary_key(appearances_csv):
    # Example when a row matches the set of primary keys
    label = {'playerID': 'aardsda01', 'G_all': '33', 'GS': '0', 'G_batting': '30', 'G_defense': '33'}
    assert appearances_csv.find_by_primary_key(['aardsda01', 'ATL', '2015'], ['playerID', 'G_all', 'GS', 'G_batting', 'G_defense']) == label

    # Example when a row matches the set of primary keys but no field_list is provided
    # All the fields are returned in this case, this mimics the database behavior of SELECT *
    label = {'GS': '0', 'G_1b': '0', 'G_2b': '0', 'G_3b': '0', 'G_all': '33', 'G_batting': '30', 'G_c': '0', 'G_cf': '0', 'G_defense': '33', 'G_dh': '0', 'G_lf': '0', 'G_of': '0', 'G_p': '33', 'G_ph': '0', 'G_pr': '0', 'G_rf': '0', 'G_ss': '0', 'lgID': 'NL', 'playerID': 'aardsda01', 'teamID': 'ATL', 'yearID': '2015'}
    assert appearances_csv.find_by_primary_key(['aardsda01', 'ATL', '2015'], None) == label
    assert appearances_csv.find_by_primary_key(['aardsda01', 'ATL', '2015'], []) == label

    # No rows match the template, returning None
    assert appearances_csv.find_by_primary_key(['aardsda01', 'ATM', '2015'], ['playerID', 'G_all', 'GS', 'G_batting', 'G_defense']) == None

    # Function fails if no key_fields are provided (None or empty) or if the dimension doesn't match with key_columns dimension
    with pytest.raises(Exception):
        appearances_csv.find_by_primary_key(None)
    with pytest.raises(Exception):
        appearances_csv.find_by_primary_key([])
    with pytest.raises(Exception):
        appearances_csv.find_by_primary_key(['1', '2'])

def test_find_by_template(appearances_csv):
    # Function fails if there is incorrect columns in template or field_list
    with pytest.raises(Exception):
        assert appearances_csv.find_by_template({'wrong': '150'}, ['playerID'])
    with pytest.raises(Exception):
        assert appearances_csv.find_by_template({'playerID': 'aardsda01'}, ['wrong'])

        # Example when some rows match the template
        label = [{'playerID': 'millake01', 'teamID': 'BOS', 'yearID': '2004'},
                 {'playerID': 'staubru01', 'teamID': 'HOU', 'yearID': '1963'},
                 {'playerID': 'wongko01', 'teamID': 'SLN', 'yearID': '2015'}]
        assert Helper.compare_two_list_of_dicts(
            appearances_csv.find_by_template({'G_all': '150', 'GS': '140', 'G_ph': '7'},
                                             ['playerID', 'teamID', 'yearID']), label)

        # Example when some rows matches the template but no field_list is provided
        # All the fields are returned in this case, this mimics the database behavior of SELECT *
        label = [{'yearID': '2004', 'teamID': 'BOS', 'lgID': 'AL', 'playerID': 'millake01', 'G_all': '150', 'GS': '140',
                  'G_batting': '150', 'G_defense': '137', 'G_p': '0', 'G_c': '0', 'G_1b': '69', 'G_2b': '0',
                  'G_3b': '0', 'G_ss': '0', 'G_lf': '20', 'G_cf': '0', 'G_rf': '55', 'G_of': '74', 'G_dh': '8',
                  'G_ph': '7', 'G_pr': '0'},
                 {'yearID': '1963', 'teamID': 'HOU', 'lgID': 'NL', 'playerID': 'staubru01', 'G_all': '150', 'GS': '140',
                  'G_batting': '150', 'G_defense': '144', 'G_p': '0', 'G_c': '0', 'G_1b': '109', 'G_2b': '0',
                  'G_3b': '0', 'G_ss': '0', 'G_lf': '0', 'G_cf': '0', 'G_rf': '49', 'G_of': '49', 'G_dh': '0',
                  'G_ph': '7', 'G_pr': '1'},
                 {'yearID': '2015', 'teamID': 'SLN', 'lgID': 'NL', 'playerID': 'wongko01', 'G_all': '150', 'GS': '140',
                  'G_batting': '150', 'G_defense': '147', 'G_p': '0', 'G_c': '0', 'G_1b': '0', 'G_2b': '147',
                  'G_3b': '0', 'G_ss': '0', 'G_lf': '0', 'G_cf': '0', 'G_rf': '0', 'G_of': '0', 'G_dh': '0',
                  'G_ph': '7', 'G_pr': '0'}]
        assert Helper.compare_two_list_of_dicts(
            appearances_csv.find_by_template({'G_all': '150', 'GS': '140', 'G_ph': '7'}), label)

    # No rows match the template, returning an empty list
    assert appearances_csv.find_by_template({'G_all': '123', 'GS': '250'}, ['playerID', 'yearID']) == []

    # If no template is provided, all the rows are returned
    # This mimics the database behavior of 'SELECT' without 'WHERE' clause
    # This test case could take a while, so I comment it (if you want to try, just uncomment it)
    # assert len(appearances_csv.find_by_template({}, ['G_p'])) == 105789

def test_insert(appearances_csv):
    # Example when insert succeeds (unique set of primary keys)
    # Check that a record doesn't exist
    assert appearances_csv.find_by_primary_key(['aardsda01', 'ABC', '2015'], ['yearID', 'teamID', 'playerID']) == None
    # Insert that record
    assert appearances_csv.insert({'yearID': '2015', 'teamID': 'ABC', 'lgID': 'NL', 'playerID': 'aardsda01', 'G_all': '33', 'GS': '0', 'G_batting': '30', 'G_defense': '33', 'G_p': '33', 'G_c': '0', 'G_1b': '0', 'G_2b': '0', 'G_3b': '0', 'G_ss': '0', 'G_lf': '0', 'G_cf': '0', 'G_rf': '0', 'G_of': '0', 'G_dh': '0', 'G_ph': '0', 'G_pr': '0'}) == None
    # Check that it exists after insertion
    assert appearances_csv.find_by_primary_key(['aardsda01', 'ABC', '2015'], ['yearID', 'teamID', 'playerID']) == {'yearID': '2015', 'teamID': 'ABC', 'playerID': 'aardsda01'}

    # Example when insert fails because of conflicting primary keys
    with pytest.raises(Exception):
        appearances_csv.insert(
            {'yearID': '2015', 'teamID': 'ATL', 'lgID': 'NL', 'playerID': 'aardsda01', 'G_all': '33', 'GS': '0',
             'G_batting': '30', 'G_defense': '33', 'G_p': '33', 'G_c': '0', 'G_1b': '0', 'G_2b': '0', 'G_3b': '0',
             'G_ss': '0', 'G_lf': '0', 'G_cf': '0', 'G_rf': '0', 'G_of': '0', 'G_dh': '0', 'G_ph': '0', 'G_pr': '0'})

    # Examples when insert fails because new_record is none, empty or has missing, incomplete or incorrect fields
    with pytest.raises(Exception):
        assert appearances_csv.insert(None)
    with pytest.raises(Exception):
        assert appearances_csv.insert({})
    with pytest.raises(Exception):
        assert appearances_csv.insert({'playerID': 'geraldi'})
    with pytest.raises(Exception):
        assert appearances_csv.insert({'wrong': '150'})

def test_delete_by_key(appearances_csv):
    # Delete fails, there is no matching row
    assert appearances_csv.delete_by_key(['aardsda01', 'ABC', '2015']) == 0
    # Insert a dummy record
    assert appearances_csv.insert({'yearID': '2015', 'teamID': 'ABC', 'lgID': 'NL', 'playerID': 'aardsda01', 'G_all': '33', 'GS': '0', 'G_batting': '30', 'G_defense': '33', 'G_p': '33', 'G_c': '0', 'G_1b': '0', 'G_2b': '0', 'G_3b': '0', 'G_ss': '0', 'G_lf': '0', 'G_cf': '0', 'G_rf': '0', 'G_of': '0', 'G_dh': '0', 'G_ph': '0', 'G_pr': '0'}) == None
    # Delete succeeds
    assert appearances_csv.delete_by_key(['aardsda01', 'ABC', '2015']) == 1
    # Check if that record doesn't exist anymore
    assert appearances_csv.find_by_primary_key(['aardsda01', 'ABC', '2015']) == None

    # Function fails if no key_fields are provided (None or empty) or if the dimension doesn't match with key_columns dimension
    with pytest.raises(Exception):
        appearances_csv.delete_by_key(None)
    with pytest.raises(Exception):
        appearances_csv.delete_by_key([])
    with pytest.raises(Exception):
        appearances_csv.delete_by_key(['1', '2'])

def test_delete_by_template(appearances_csv):
    # Delete fails, there is no matching row
    assert appearances_csv.delete_by_template({'GS': '999'}) == 0
    # Count the number of records that match a specific template
    assert len(appearances_csv.find_by_template({'GS': '150', 'G_all': '150'})) == 83
    # Delete them
    assert appearances_csv.delete_by_template({'GS': '150', 'G_all': '150'}) == 83
    # Count them again, it should be zero
    assert len(appearances_csv.find_by_template({'GS': '150', 'G_all': '150'})) == 0

    # If no template is provided (None or empty), ALL ROWS are deleted
    # This mimics the behavior of 'DELETE' without 'WHERE' clause
    assert appearances_csv.delete_by_template({}) == 105789 - 83
    # Database now doesn't have any rows
    assert len(appearances_csv.find_by_template({})) == 0

    # Function fails if there is incorrect columns in template or field_list
    with pytest.raises(Exception):
        assert appearances_csv.delete_by_template({'wrong': '150'})

def test_update_by_key(appearances_csv):
    # Example when update fails because of conflicting primary keys
    # Find all yearIDs where 'aaronha01' plays for 'ATL'. Amongst those years, there are '1969' and '1970'.
    assert appearances_csv.find_by_template({'playerID': 'aaronha01', 'teamID': 'ATL'}, ['yearID']) == [{'yearID': '1966'}, {'yearID': '1967'}, {'yearID': '1968'}, {'yearID': '1969'}, {'yearID': '1970'}, {'yearID': '1971'}, {'yearID': '1972'}, {'yearID': '1973'}, {'yearID': '1974'}]
    # Try change the record that has year '1970' to '1969', it should fail
    with pytest.raises(Exception):
        appearances_csv.update_by_key(['aaronha01', 'ATL', '1970'], {'yearID': '1969'})
    # Try change the record that has year '1970' to '1900', it should succeed because '1900' was not amongst the result before.
    assert appearances_csv.update_by_key(['aaronha01', 'ATL', '1970'], {'yearID': '1900'}) == 1

    # Example when record is not found (it has been updated already), return 0
    assert appearances_csv.update_by_key(['aaronha01', 'ATL', '1970'], {'yearID': '1900'}) == 0

    # Function directly returns zero if new_values is None or empty, as nothing to update
    assert appearances_csv.update_by_key(['aardsda01', 'ATL', '2015'], {}) == 0

    # Function fails if no key_fields are provided (None or empty) or if the dimension doesn't match with key_columns dimension
    with pytest.raises(Exception):
        appearances_csv.update_by_key([], {})
    with pytest.raises(Exception):
        appearances_csv.update_by_key(['1', '2'], {})

def test_update_by_template(appearances_csv):
    # Find some kind of appearances
    assert len(appearances_csv.find_by_template({'teamID': 'ATL', 'yearID': '1970', 'G_p': '0'})) == 20
    # Try change 'G_p' from '0' to '1'
    assert appearances_csv.update_by_template({'teamID': 'ATL', 'yearID': '1970', 'G_p': '0'}, {'G_p': '1'}) == 20
    # Find updated appearances with 'G_p' equals '1'
    assert len(appearances_csv.find_by_template({'teamID': 'ATL', 'yearID': '1970', 'G_p': '1'})) == 20

    # Example when records are not found (they are updated already), return 0
    assert appearances_csv.update_by_template({'teamID': 'ATL', 'yearID': '1970', 'G_p': '0'}, {'G_p': '1'}) == 0

    # Function directly returns zero if new_values is None or empty, as nothing to update
    assert appearances_csv.update_by_template({'teamID': 'ATL', 'yearID': '1970', 'G_p': '1'}, {}) == 0

    # If we change all the playerIDs that play for 'ATL' in '1970' to one of the playerID,
    # it will cause duplicate primary keys and raise Exception
    with pytest.raises(Exception):
        appearances_csv.update_by_template({'teamID': 'ATL', 'yearID': '1970'}, {'playerID': 'aaronha01'})

    # If no template is provided (None or empty), ALL ROWS are updated
    # This mimics the behavior of 'UPDATE' without 'WHERE' clause
    assert appearances_csv.update_by_template({}, {'G_p': '123'}) == 105789
    assert len(appearances_csv.find_by_template({'G_p': '123'})) == 105789

    # Function fails if there is incorrect columns in template or field_list
    with pytest.raises(Exception):
        assert appearances_csv.delete_by_template({'wrong': '150'})

def test_no_primary_key(appearances_csv):
    # All by_key methods should fail if table has no primary keys
    appearances_csv._data['key_columns'] = None
    with pytest.raises(Exception):
        assert appearances_csv.find_by_primary_key(None) == None
    with pytest.raises(Exception):
        assert appearances_csv.delete_by_key(None) == 0
    with pytest.raises(Exception):
        assert appearances_csv.update_by_key(None, []) == 0

    appearances_csv._data['key_columns'] = []
    with pytest.raises(Exception):
        assert appearances_csv.find_by_primary_key([]) == None
    with pytest.raises(Exception):
        assert appearances_csv.delete_by_key([]) == 0
    with pytest.raises(Exception):
        assert appearances_csv.update_by_key([], []) == 0