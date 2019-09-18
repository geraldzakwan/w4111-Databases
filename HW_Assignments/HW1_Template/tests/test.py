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

# Check if two list of dicts are the same, regardless the order
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

@pytest.fixture
def appearances_rdb():
    return RDBDataTable(
        "appearances", {
            "host": "localhost",
            "user": "dbuser",
            "password": "dbuser",
            "db": "lahman2019"
        }, [
            "playerID",
            "teamID",
            "yearID"
        ]
    )

def test_find_by_primary_key(appearances_csv, appearances_rdb):
    # If no key_fields provided, I assume I should return None
    assert appearances_csv.find_by_primary_key(None) == None
    assert appearances_csv.find_by_primary_key([]) == None
    assert appearances_rdb.find_by_primary_key(None) == None
    assert appearances_rdb.find_by_primary_key([]) == None

    # A row matches the set of primary keys
    label = {"playerID": "aardsda01", "G_all": "33", "GS": "0", "G_batting": "30", "G_defense": "33"}
    assert appearances_csv.find_by_primary_key(["aardsda01", "ATL", "2015"], ["playerID", "G_all", "GS", "G_batting", "G_defense"]) == label
    assert appearances_rdb.find_by_primary_key(["aardsda01", "ATL", "2015"], ["playerID", "G_all", "GS", "G_batting", "G_defense"]) == label

    # A row matches the set of primary keys but no field_list is provided
    # I assume I just return an empty dictionary in this case
    assert appearances_csv.find_by_primary_key(["aardsda01", "ATL", "2015"], None) == {}
    assert appearances_csv.find_by_primary_key(["aardsda01", "ATL", "2015"], []) == {}
    assert appearances_rdb.find_by_primary_key(["aardsda01", "ATL", "2015"], None) == {}
    assert appearances_rdb.find_by_primary_key(["aardsda01", "ATL", "2015"], []) == {}

    # No rows match the set of primary keys
    assert appearances_csv.find_by_primary_key(["aardsda01", "ATM", "2015"], ["playerID", "G_all", "GS", "G_batting", "G_defense"]) == None
    assert appearances_rdb.find_by_primary_key(["aardsda01", "ATM", "2015"], ["playerID", "G_all", "GS", "G_batting", "G_defense"]) == None

def test_find_by_template(appearances_csv, appearances_rdb):
    # If no key_fields provided, I assume I should just return an empty list
    assert appearances_csv.find_by_template(None) == []
    assert appearances_csv.find_by_template([]) == []
    assert appearances_rdb.find_by_template(None) == []
    assert appearances_rdb.find_by_template([]) == []

    #Q How do we compare the set? Sometimes elements' position are not the same
    # Rows match the template
    label = [{"playerID": "millake01", "teamID": "BOS", "yearID": "2004"}, {"playerID": "staubru01", "teamID": "HOU", "yearID": "1963"}, {"playerID": "wongko01", "teamID": "SLN", "yearID": "2015"}]
    assert compare_two_list_of_dicts(appearances_csv.find_by_template({"G_all": "150", "GS": "140", "G_ph": "7"}, ["playerID", "teamID", "yearID"]), label)
    assert compare_two_list_of_dicts(appearances_rdb.find_by_template({"G_all": "150", "GS": "140", "G_ph": "7"}, ["playerID", "teamID", "yearID"]), label)

    # Row matches the set of primary keys but no field_list is provided
    # I assume I just return an empty dictionary for each element of the list in this case
    label = [{}, {}, {}]
    assert appearances_csv.find_by_template({"G_all": "150", "GS": "140", "G_ph": "7"}) == label
    assert appearances_rdb.find_by_template({"G_all": "150", "GS": "140", "G_ph": "7"}) == label

    # No rows match the template, returning empty list
    assert appearances_csv.find_by_template({"G_all": "123", "GS": "250"}, ["playerID", "yearID"]) == []
    assert appearances_rdb.find_by_template({"G_all": "123", "GS": "250"}, ["playerID", "yearID"]) == []
