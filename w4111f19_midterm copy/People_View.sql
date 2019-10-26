-- %%sql
-- - UNI
--     - last_name
--     - first_name
--     - Type is 'S' if the person is a student and 'F' if the person is a faculty.
--     - 'NA' for graduation year if the person is not a student.
--     - 'NA' for title if the person is not a faculty.
USE classicmodels;

-- DROP TABLE IF EXISTS People;

-- CREATE VIEW People (uni, last_name, first_name, type, graduation_year, title) AS
-- 	SELECT student as uni, last_name, first_name, 'S', graduation_year, 'NA' FROM student
-- 	UNION ALL
-- 	SELECT uni, last_name, first_name, 'F', 'NA', title FROM faculty;
--     
SELECT * FROM People;
-- DESC People;

-- DESC People;

-- INSERT INTO `student` VALUES ('gd2551', 'dzakwan', 'geraldi', '2020');
-- INSERT INTO `faculty` VALUES ('yb2235', 'benajiba', 'yassine', 'Adjunct Professor');