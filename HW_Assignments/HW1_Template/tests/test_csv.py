# I write and test methods one at a time.
# This file contains unit tests of individual methods.

from src.CSVDataTable import CSVDataTable
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

@pytest.fixture
def people_csv():
    connect_info = {
        "directory": data_dir,
        "file_name": "People.csv"
    }

    csv_tbl = CSVDataTable("people", connect_info, ["playerID"])

    print("Created table = " + str(csv_tbl))

    return csv_tbl

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

def test_find_by_primary_key(people_csv):
    # assert people_csv.find_by_primary_key(None) == people_csv.get_rows()
    # assert people_csv.find_by_primary_key([]) == people_csv.get_rows()
    assert people_csv.find_by_primary_key(None) == None
    assert people_csv.find_by_primary_key([]) == {}
    assert people_csv.find_by_primary_key(["aardsda01"], ["birthYear", "birthMonth"]) == {"birthYear": "1981", "birthMonth": "12"}
    assert people_csv.find_by_primary_key(["aardsda01"]) == {}
    assert people_csv.find_by_primary_key(["aardsda07"]) == None

def test_find_by_template(people_csv):
    # assert people_csv.find_by_template(None) == people_csv.get_rows()
    # assert people_csv.find_by_template({}) == people_csv.get_rows()
    assert people_csv.find_by_template(None) == None
    assert people_csv.find_by_template({}) == {}
    assert people_csv.find_by_template({"birthYear": "1985"}, ["birthYear", "birthMonth"]) == [{'birthYear': '1985', 'birthMonth': '12'}, {'birthYear': '1985', 'birthMonth': '4'}, {'birthYear': '1985', 'birthMonth': '5'}, {'birthYear': '1985', 'birthMonth': '10'}, {'birthYear': '1985', 'birthMonth': '9'}, {'birthYear': '1985', 'birthMonth': '4'}, {'birthYear': '1985', 'birthMonth': '5'}, {'birthYear': '1985', 'birthMonth': '5'}, {'birthYear': '1985', 'birthMonth': '10'}, {'birthYear': '1985', 'birthMonth': '7'}, {'birthYear': '1985', 'birthMonth': '6'}, {'birthYear': '1985', 'birthMonth': '11'}, {'birthYear': '1985', 'birthMonth': '8'}, {'birthYear': '1985', 'birthMonth': '9'}, {'birthYear': '1985', 'birthMonth': '1'}, {'birthYear': '1985', 'birthMonth': '11'}, {'birthYear': '1985', 'birthMonth': '9'}, {'birthYear': '1985', 'birthMonth': '4'}, {'birthYear': '1985', 'birthMonth': '11'}, {'birthYear': '1985', 'birthMonth': '3'}, {'birthYear': '1985', 'birthMonth': '4'}, {'birthYear': '1985', 'birthMonth': '4'}, {'birthYear': '1985', 'birthMonth': '12'}, {'birthYear': '1985', 'birthMonth': '6'}, {'birthYear': '1985', 'birthMonth': '11'}, {'birthYear': '1985', 'birthMonth': '1'}, {'birthYear': '1985', 'birthMonth': '11'}, {'birthYear': '1985', 'birthMonth': '5'}, {'birthYear': '1985', 'birthMonth': '12'}, {'birthYear': '1985', 'birthMonth': '7'}, {'birthYear': '1985', 'birthMonth': '11'}, {'birthYear': '1985', 'birthMonth': '9'}, {'birthYear': '1985', 'birthMonth': '1'}, {'birthYear': '1985', 'birthMonth': '10'}, {'birthYear': '1985', 'birthMonth': '9'}, {'birthYear': '1985', 'birthMonth': '7'}, {'birthYear': '1985', 'birthMonth': '9'}, {'birthYear': '1985', 'birthMonth': '2'}, {'birthYear': '1985', 'birthMonth': '6'}, {'birthYear': '1985', 'birthMonth': '9'}, {'birthYear': '1985', 'birthMonth': '1'}, {'birthYear': '1985', 'birthMonth': '8'}, {'birthYear': '1985', 'birthMonth': '2'}, {'birthYear': '1985', 'birthMonth': '4'}, {'birthYear': '1985', 'birthMonth': '9'}, {'birthYear': '1985', 'birthMonth': '10'}, {'birthYear': '1985', 'birthMonth': '10'}, {'birthYear': '1985', 'birthMonth': '10'}, {'birthYear': '1985', 'birthMonth': '8'}, {'birthYear': '1985', 'birthMonth': '9'}, {'birthYear': '1985', 'birthMonth': '2'}, {'birthYear': '1985', 'birthMonth': '8'}, {'birthYear': '1985', 'birthMonth': '4'}, {'birthYear': '1985', 'birthMonth': '12'}, {'birthYear': '1985', 'birthMonth': '5'}, {'birthYear': '1985', 'birthMonth': '12'}, {'birthYear': '1985', 'birthMonth': '6'}, {'birthYear': '1985', 'birthMonth': '8'}, {'birthYear': '1985', 'birthMonth': '10'}, {'birthYear': '1985', 'birthMonth': '8'}, {'birthYear': '1985', 'birthMonth': '11'}, {'birthYear': '1985', 'birthMonth': '8'}, {'birthYear': '1985', 'birthMonth': '6'}, {'birthYear': '1985', 'birthMonth': '11'}, {'birthYear': '1985', 'birthMonth': '10'}, {'birthYear': '1985', 'birthMonth': '9'}, {'birthYear': '1985', 'birthMonth': '7'}, {'birthYear': '1985', 'birthMonth': '8'}, {'birthYear': '1985', 'birthMonth': '12'}, {'birthYear': '1985', 'birthMonth': '7'}, {'birthYear': '1985', 'birthMonth': '4'}, {'birthYear': '1985', 'birthMonth': '8'}, {'birthYear': '1985', 'birthMonth': '1'}, {'birthYear': '1985', 'birthMonth': '4'}, {'birthYear': '1985', 'birthMonth': '1'}, {'birthYear': '1985', 'birthMonth': '9'}, {'birthYear': '1985', 'birthMonth': '12'}, {'birthYear': '1985', 'birthMonth': '10'}, {'birthYear': '1985', 'birthMonth': '9'}, {'birthYear': '1985', 'birthMonth': '10'}, {'birthYear': '1985', 'birthMonth': '1'}, {'birthYear': '1985', 'birthMonth': '8'}, {'birthYear': '1985', 'birthMonth': '11'}, {'birthYear': '1985', 'birthMonth': '6'}, {'birthYear': '1985', 'birthMonth': '9'}, {'birthYear': '1985', 'birthMonth': '11'}, {'birthYear': '1985', 'birthMonth': '1'}, {'birthYear': '1985', 'birthMonth': '8'}, {'birthYear': '1985', 'birthMonth': '5'}, {'birthYear': '1985', 'birthMonth': '9'}, {'birthYear': '1985', 'birthMonth': '2'}, {'birthYear': '1985', 'birthMonth': '9'}, {'birthYear': '1985', 'birthMonth': '3'}, {'birthYear': '1985', 'birthMonth': '11'}, {'birthYear': '1985', 'birthMonth': '10'}, {'birthYear': '1985', 'birthMonth': '4'}, {'birthYear': '1985', 'birthMonth': '7'}, {'birthYear': '1985', 'birthMonth': '9'}, {'birthYear': '1985', 'birthMonth': '6'}, {'birthYear': '1985', 'birthMonth': '3'}, {'birthYear': '1985', 'birthMonth': '8'}, {'birthYear': '1985', 'birthMonth': '8'}, {'birthYear': '1985', 'birthMonth': '6'}, {'birthYear': '1985', 'birthMonth': '4'}, {'birthYear': '1985', 'birthMonth': '1'}, {'birthYear': '1985', 'birthMonth': '5'}, {'birthYear': '1985', 'birthMonth': '1'}, {'birthYear': '1985', 'birthMonth': '3'}, {'birthYear': '1985', 'birthMonth': '10'}, {'birthYear': '1985', 'birthMonth': '3'}, {'birthYear': '1985', 'birthMonth': '6'}, {'birthYear': '1985', 'birthMonth': '2'}, {'birthYear': '1985', 'birthMonth': '12'}, {'birthYear': '1985', 'birthMonth': '1'}, {'birthYear': '1985', 'birthMonth': '4'}, {'birthYear': '1985', 'birthMonth': '3'}, {'birthYear': '1985', 'birthMonth': '8'}, {'birthYear': '1985', 'birthMonth': '9'}, {'birthYear': '1985', 'birthMonth': '5'}, {'birthYear': '1985', 'birthMonth': '4'}, {'birthYear': '1985', 'birthMonth': '3'}, {'birthYear': '1985', 'birthMonth': '10'}, {'birthYear': '1985', 'birthMonth': '3'}, {'birthYear': '1985', 'birthMonth': '5'}, {'birthYear': '1985', 'birthMonth': '1'}, {'birthYear': '1985', 'birthMonth': '4'}, {'birthYear': '1985', 'birthMonth': '3'}, {'birthYear': '1985', 'birthMonth': '2'}, {'birthYear': '1985', 'birthMonth': '9'}, {'birthYear': '1985', 'birthMonth': '4'}, {'birthYear': '1985', 'birthMonth': '5'}, {'birthYear': '1985', 'birthMonth': '5'}, {'birthYear': '1985', 'birthMonth': '4'}, {'birthYear': '1985', 'birthMonth': '9'}, {'birthYear': '1985', 'birthMonth': '3'}, {'birthYear': '1985', 'birthMonth': '11'}, {'birthYear': '1985', 'birthMonth': '2'}, {'birthYear': '1985', 'birthMonth': '4'}, {'birthYear': '1985', 'birthMonth': '2'}, {'birthYear': '1985', 'birthMonth': '11'}, {'birthYear': '1985', 'birthMonth': '8'}, {'birthYear': '1985', 'birthMonth': '2'}, {'birthYear': '1985', 'birthMonth': '11'}, {'birthYear': '1985', 'birthMonth': '6'}, {'birthYear': '1985', 'birthMonth': '10'}, {'birthYear': '1985', 'birthMonth': '9'}, {'birthYear': '1985', 'birthMonth': '2'}, {'birthYear': '1985', 'birthMonth': '7'}, {'birthYear': '1985', 'birthMonth': '7'}, {'birthYear': '1985', 'birthMonth': '1'}, {'birthYear': '1985', 'birthMonth': '2'}, {'birthYear': '1985', 'birthMonth': '7'}, {'birthYear': '1985', 'birthMonth': '2'}, {'birthYear': '1985', 'birthMonth': '7'}, {'birthYear': '1985', 'birthMonth': '8'}, {'birthYear': '1985', 'birthMonth': '10'}, {'birthYear': '1985', 'birthMonth': '5'}, {'birthYear': '1985', 'birthMonth': '12'}, {'birthYear': '1985', 'birthMonth': '4'}, {'birthYear': '1985', 'birthMonth': '7'}, {'birthYear': '1985', 'birthMonth': '4'}, {'birthYear': '1985', 'birthMonth': '9'}, {'birthYear': '1985', 'birthMonth': '2'}, {'birthYear': '1985', 'birthMonth': '12'}, {'birthYear': '1985', 'birthMonth': '4'}, {'birthYear': '1985', 'birthMonth': '8'}, {'birthYear': '1985', 'birthMonth': '9'}, {'birthYear': '1985', 'birthMonth': '1'}, {'birthYear': '1985', 'birthMonth': '12'}, {'birthYear': '1985', 'birthMonth': '8'}, {'birthYear': '1985', 'birthMonth': '9'}, {'birthYear': '1985', 'birthMonth': '3'}, {'birthYear': '1985', 'birthMonth': '8'}, {'birthYear': '1985', 'birthMonth': '5'}, {'birthYear': '1985', 'birthMonth': '5'}, {'birthYear': '1985', 'birthMonth': '1'}, {'birthYear': '1985', 'birthMonth': '11'}, {'birthYear': '1985', 'birthMonth': '1'}, {'birthYear': '1985', 'birthMonth': '12'}, {'birthYear': '1985', 'birthMonth': '9'}, {'birthYear': '1985', 'birthMonth': '7'}, {'birthYear': '1985', 'birthMonth': '7'}, {'birthYear': '1985', 'birthMonth': '1'}, {'birthYear': '1985', 'birthMonth': '8'}, {'birthYear': '1985', 'birthMonth': '7'}, {'birthYear': '1985', 'birthMonth': '5'}, {'birthYear': '1985', 'birthMonth': '12'}, {'birthYear': '1985', 'birthMonth': '4'}, {'birthYear': '1985', 'birthMonth': '6'}, {'birthYear': '1985', 'birthMonth': '12'}, {'birthYear': '1985', 'birthMonth': '9'}, {'birthYear': '1985', 'birthMonth': '2'}, {'birthYear': '1985', 'birthMonth': '12'}, {'birthYear': '1985', 'birthMonth': '11'}, {'birthYear': '1985', 'birthMonth': '8'}, {'birthYear': '1985', 'birthMonth': '11'}, {'birthYear': '1985', 'birthMonth': '3'}, {'birthYear': '1985', 'birthMonth': '5'}, {'birthYear': '1985', 'birthMonth': '6'}, {'birthYear': '1985', 'birthMonth': '3'}, {'birthYear': '1985', 'birthMonth': '10'}, {'birthYear': '1985', 'birthMonth': '9'}, {'birthYear': '1985', 'birthMonth': '3'}, {'birthYear': '1985', 'birthMonth': '5'}, {'birthYear': '1985', 'birthMonth': '12'}, {'birthYear': '1985', 'birthMonth': '8'}, {'birthYear': '1985', 'birthMonth': '3'}, {'birthYear': '1985', 'birthMonth': '7'}, {'birthYear': '1985', 'birthMonth': '9'}, {'birthYear': '1985', 'birthMonth': '1'}, {'birthYear': '1985', 'birthMonth': '9'}, {'birthYear': '1985', 'birthMonth': '5'}, {'birthYear': '1985', 'birthMonth': '5'}]
    assert people_csv.find_by_template({"birthYear": "1985", "birthMonth": "5"}, ["birthYear", "birthMonth"]) == [{'birthYear': '1985', 'birthMonth': '5'}, {'birthYear': '1985', 'birthMonth': '5'}, {'birthYear': '1985', 'birthMonth': '5'}, {'birthYear': '1985', 'birthMonth': '5'}, {'birthYear': '1985', 'birthMonth': '5'}, {'birthYear': '1985', 'birthMonth': '5'}, {'birthYear': '1985', 'birthMonth': '5'}, {'birthYear': '1985', 'birthMonth': '5'}, {'birthYear': '1985', 'birthMonth': '5'}, {'birthYear': '1985', 'birthMonth': '5'}, {'birthYear': '1985', 'birthMonth': '5'}, {'birthYear': '1985', 'birthMonth': '5'}, {'birthYear': '1985', 'birthMonth': '5'}, {'birthYear': '1985', 'birthMonth': '5'}, {'birthYear': '1985', 'birthMonth': '5'}, {'birthYear': '1985', 'birthMonth': '5'}, {'birthYear': '1985', 'birthMonth': '5'}, {'birthYear': '1985', 'birthMonth': '5'}, {'birthYear': '1985', 'birthMonth': '5'}]
    assert people_csv.find_by_template({"birthYear": "1585"}, ["birthYear", "birthMonth"]) == []

def test_delete_by_key(people_csv):
    assert people_csv.delete_by_key(None) == 0
    assert people_csv.delete_by_key([]) == 0
    assert people_csv.delete_by_key(["aardsda01"]) == 1
    assert people_csv.delete_by_key(["aardsda07"]) == 0

def test_delete_by_template(people_csv):
    assert people_csv.delete_by_template(None) == 0
    assert people_csv.delete_by_template({}) == 0
    assert people_csv.delete_by_template({"playerID": "aardsda01"}) == 1
    assert people_csv.delete_by_template({"birthYear": "1585"}) == 0
    assert people_csv.delete_by_template({"birthYear": "1985", "birthMonth": "0"}) == 0
    assert len(people_csv.find_by_template({"birthYear": "1985"})) == 213
    assert people_csv.delete_by_template({"birthYear": "1985", "birthMonth": "5"}) == 19
    assert people_csv.delete_by_template({"birthYear": "1985"}) == 194

def test_insert(people_csv):
    initial_length = len(people_csv.get_rows())
    people_csv.insert(None)
    assert len(people_csv.get_rows()) == initial_length
    people_csv.insert({})
    assert len(people_csv.get_rows()) == initial_length

    # assert people_csv.delete_by_template({"playerID": "aardsda01"}) == 1
    # assert people_csv.delete_by_template({"birthYear": "1585"}) == 0
    # assert people_csv.delete_by_template({"birthYear": "1985", "birthMonth": "0"}) == 0
    # assert len(people_csv.find_by_template({"birthYear": "1985"})) == 213
    # assert people_csv.delete_by_template({"birthYear": "1985", "birthMonth": "5"}) == 19
    # assert people_csv.delete_by_template({"birthYear": "1985"}) == 194

# def test_save(people_csv):
#     # Delete 1 row by key
#     # Reload and the len of rows should be 1 short
#     pass
