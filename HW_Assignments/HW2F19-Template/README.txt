This README consists of three parts:

1. Setup
2. Testing Detail
3. Design Decisions/Implementations

1. Setup
    a. Install requirement: pip install -r requirements.txt
        - This is to install Pandas, PyMySQL and Flask
        - I don't use Pandas library but since it is included in started code, I keep it as is

    b. If using PyCharm, set root to the directory above src and test

2. Testing Detail:
    a. To test the API, i.e. app.py, I use POSTMAN and I include all the test results
       in a PDF file under the test directory -> test/api_test_result.pdf

    b. To test get_primary_key_columns() and get_row_count() functions implemented in RDBDataTable.py,
       I create a unit test file under the test directory -> test/test_rdb_data_table.py.

       - It can be run directly in PyCharm -> right click the unit test file and choose Run

       - Explanations and assumptions about test scenarios are included in the unit test file as comments.

    c. I am not testing get_databases() and get_tables() functions implemented in data_table_adaptor.py
       because they are already tested when I test the API, i.e. when accessing
       /api/databases and /api/databases/<dbname> routes

3. Design Decisions/Implementations:
    - Regarding _default_connect_info variable in RDBDataTable module

      I change the field 'user' from originally having 'root' value to having 'dbuser' value
      Reference: https://piazza.com/class/jy3jm0i73f8584?cid=450

    - Regarding /api/databases and /api/databases/<dbname> routes

       1. The two routes don't return all databases and tables in your MySQL environment.
          Instead, they just return the ones that are already stored in cache (have been accessed before),
          i.e. in the _db_tables dictionary in data_table_adaptor module
          Reference: https://piazza.com/class/jy3jm0i73f8584?cid=435

       2. In the case of empty cache, an empty list will be returned for both
          /api/databases and /api/databases/<dbname>

    - Regarding the use of status code 404 NOT FOUND

       This status code will be returned in the case of the followings:

       1. No resource is found when calling GET by primary key route, e.g. GET /api/lahman2019clean/people/willite02
          In this case, there is no playerID equals to willite02, so 404 is returned

       2. No resource is found when calling GET by template, e.g.
          GET /api/lahman2019clean/batting?teamID=BOS&yearID=1960&AB=123&H=0&fields=playerID,AB,H

       3. No resource is found when calling UPDATE by primary key, e.g. PUT /api/lahman2019clean/people/willite02

    - Regarding the use of status code 500 INTERNAL ERROR

       This status code will be returned in the case of the followings:

       1. Calling operations that can violate primary key constraints:

          a. DELETE a row whose primary key is referred by other rows in other tables,
          e.g. DELETE /api/lahman2019clean/people/willite01

          b. INSERT a row that causes duplication of primary key,
          e.g. INSERT /api/lahman2019clean/people with body {'playerID: willite01', 'nameLast': 'New Williams'}

       2. Calling any other operations that cause MySQL to throw an error, for example:

          a. When we try to INSERT a row in which some column names are invalid:
          INSERT with body {'player_ID': 'gerald82'} -> 'player_ID' is not a valid column, it should be 'playerID'

          b. When we specify database/table that doesn't exist:
          GET /api/lahman2019clean/battings?teamID=BOS -> table Battings doesn't exist, it should be Batting

    - Regarding response message

       For routes that don't return resource, e.g. POST, PUT, DELETE and GET when failed,
       generally, these are my responses format in JSON:

       a. For status code 200 (success request):

            {
                'status_code': 200,
                'msg': 'Some messages, depending on the case'
            }

       b. For status code 404 (resource not found) or 500 (internal error):

            {
                'status_code': 404 / 500,
                'err_msg': 'Some error messages, depending on the case'
            }

       The detail (case by case) is shown in the PDF file ->    test/api_test_result.pdf