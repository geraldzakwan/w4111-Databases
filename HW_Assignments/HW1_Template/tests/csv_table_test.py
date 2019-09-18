# I write and test methods one at a time.
# This file contains unit tests of individual methods.

from src.CSVDataTable import CSVDataTable
from src.RDBDataTable import RDBDataTable
import logging
import os
import pytest

# The logging level to use should be an environment variable, not hard coded.
logging.basicConfig(level=logging.DEBUG)

# Also, the 'name' of the logger to use should be an environment variable.
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

# This should also be an environment variable.
# Also not the using '/' is OS dependent, and windows might need `\\`
# data_dir = os.path.abspath("../Data/Baseball")
data_dir = os.path.abspath("Data/Baseball")

# Check if two list of dicts are the same, regardless of the order of the elements
def compare_two_list_of_dicts(a, b):
    # Get random field from field_list
    random_field = next(iter(a[0]))

    new_list_a = sorted(a, key=lambda k: k[random_field])
    new_list_b = sorted(b, key=lambda k: k[random_field])

    return new_list_a == new_list_b

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
    # If no key_fields provided, I assume I should return None
    assert appearances_csv.find_by_primary_key(None) == None
    assert appearances_csv.find_by_primary_key([]) == None

    # A row matches the set of primary keys
    label = {"playerID": "aardsda01", "G_all": "33", "GS": "0", "G_batting": "30", "G_defense": "33"}
    assert appearances_csv.find_by_primary_key(["aardsda01", "ATL", "2015"], ["playerID", "G_all", "GS", "G_batting", "G_defense"]) == label

    # A row matches the set of primary keys but no field_list is provided
    # I assume I just return an empty dictionary in this case
    label = {'GS': '0', 'G_1b': '0', 'G_2b': '0', 'G_3b': '0', 'G_all': '33', 'G_batting': '30', 'G_c': '0', 'G_cf': '0', 'G_defense': '33', 'G_dh': '0', 'G_lf': '0', 'G_of': '0', 'G_p': '33', 'G_ph': '0', 'G_pr': '0', 'G_rf': '0', 'G_ss': '0', 'lgID': 'NL', 'playerID': 'aardsda01', 'teamID': 'ATL', 'yearID': '2015'}
    assert appearances_csv.find_by_primary_key(["aardsda01", "ATL", "2015"], None) == label
    assert appearances_csv.find_by_primary_key(["aardsda01", "ATL", "2015"], []) == label

    # No rows match the set of primary keys
    assert appearances_csv.find_by_primary_key(["aardsda01", "ATM", "2015"], ["playerID", "G_all", "GS", "G_batting", "G_defense"]) == None

def test_find_by_template(appearances_csv):
    # If no key_fields provided, I assume I should just return an empty list
    assert appearances_csv.find_by_template(None) == []
    assert appearances_csv.find_by_template([]) == []

    #Q How do we compare the set? Sometimes elements' position are not the same
    # Rows match the template
    label = [{"playerID": "millake01", "teamID": "BOS", "yearID": "2004"}, {"playerID": "staubru01", "teamID": "HOU", "yearID": "1963"}, {"playerID": "wongko01", "teamID": "SLN", "yearID": "2015"}]
    assert compare_two_list_of_dicts(appearances_csv.find_by_template({"G_all": "150", "GS": "140", "G_ph": "7"}, ["playerID", "teamID", "yearID"]), label)

    # Row matches the set of primary keys but no field_list is provided
    # I assume I just return a whole row for each element of the list in this case
    label = [{'yearID': '2004', 'teamID': 'BOS', 'lgID': 'AL', 'playerID': 'millake01', 'G_all': '150', 'GS': '140', 'G_batting': '150', 'G_defense': '137', 'G_p': '0', 'G_c': '0', 'G_1b': '69', 'G_2b': '0', 'G_3b': '0', 'G_ss': '0', 'G_lf': '20', 'G_cf': '0', 'G_rf': '55', 'G_of': '74', 'G_dh': '8', 'G_ph': '7', 'G_pr': '0'}, {'yearID': '1963', 'teamID': 'HOU', 'lgID': 'NL', 'playerID': 'staubru01', 'G_all': '150', 'GS': '140', 'G_batting': '150', 'G_defense': '144', 'G_p': '0', 'G_c': '0', 'G_1b': '109', 'G_2b': '0', 'G_3b': '0', 'G_ss': '0', 'G_lf': '0', 'G_cf': '0', 'G_rf': '49', 'G_of': '49', 'G_dh': '0', 'G_ph': '7', 'G_pr': '1'}, {'yearID': '2015', 'teamID': 'SLN', 'lgID': 'NL', 'playerID': 'wongko01', 'G_all': '150', 'GS': '140', 'G_batting': '150', 'G_defense': '147', 'G_p': '0', 'G_c': '0', 'G_1b': '0', 'G_2b': '147', 'G_3b': '0', 'G_ss': '0', 'G_lf': '0', 'G_cf': '0', 'G_rf': '0', 'G_of': '0', 'G_dh': '0', 'G_ph': '7', 'G_pr': '0'}]
    # assert compare_two_list_of_dicts(appearances_csv.find_by_template({"G_all": "150", "GS": "140", "G_ph": "7"}), label)

    # No rows match the template, returning empty list
    assert appearances_csv.find_by_template({"G_all": "123", "GS": "250"}, ["playerID", "yearID"]) == []

def test_insert(appearances_csv):
    # Insert row with conflicting primary key, fails
    with pytest.raises(Exception):
        appearances_csv.insert({'yearID': '2015', 'teamID': 'ATL', 'lgID': 'NL', 'playerID': 'aardsda01', 'G_all': '33', 'GS': '0', 'G_batting': '30', 'G_defense': '33', 'G_p': '33', 'G_c': '0', 'G_1b': '0', 'G_2b': '0', 'G_3b': '0', 'G_ss': '0', 'G_lf': '0', 'G_cf': '0', 'G_rf': '0', 'G_of': '0', 'G_dh': '0', 'G_ph': '0', 'G_pr': '0'})

    # Insert row fails because record is none, empty or has missing fields
    with pytest.raises(Exception):
        assert appearances_csv.insert(None)
    with pytest.raises(Exception):
        assert appearances_csv.insert({})
    with pytest.raises(Exception):
        assert appearances_csv.insert({'playerID': 'geraldi'})

    # Insert row with unique primary key
    assert appearances_csv.find_by_primary_key(["aardsda01", "ABC", "2015"], ["yearID", "teamID", "playerID"]) == None
    assert appearances_csv.insert({'yearID': '2015', 'teamID': 'ABC', 'lgID': 'NL', 'playerID': 'aardsda01', 'G_all': '33', 'GS': '0', 'G_batting': '30', 'G_defense': '33', 'G_p': '33', 'G_c': '0', 'G_1b': '0', 'G_2b': '0', 'G_3b': '0', 'G_ss': '0', 'G_lf': '0', 'G_cf': '0', 'G_rf': '0', 'G_of': '0', 'G_dh': '0', 'G_ph': '0', 'G_pr': '0'}) == None
    assert appearances_csv.find_by_primary_key(["aardsda01", "ABC", "2015"], ["yearID", "teamID", "playerID"]) == {'yearID': '2015', 'teamID': 'ABC', 'playerID': 'aardsda01'}

def test_delete_by_key(appearances_csv):
    # Fail delete, no matching row
    assert appearances_csv.delete_by_key(["aardsda01", "ABC", "2015"]) == 0
    # Insert dummy row
    assert appearances_csv.insert({'yearID': '2015', 'teamID': 'ABC', 'lgID': 'NL', 'playerID': 'aardsda01', 'G_all': '33', 'GS': '0', 'G_batting': '30', 'G_defense': '33', 'G_p': '33', 'G_c': '0', 'G_1b': '0', 'G_2b': '0', 'G_3b': '0', 'G_ss': '0', 'G_lf': '0', 'G_cf': '0', 'G_rf': '0', 'G_of': '0', 'G_dh': '0', 'G_ph': '0', 'G_pr': '0'}) == None
    # Delete succesfully
    assert appearances_csv.delete_by_key(["aardsda01", "ABC", "2015"]) == 1

#def test_save():
    #pass