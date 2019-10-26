%%sql

CREATE DEFINER=`dbuser`@`localhost` PROCEDURE `create_person`(
    in last_name varchar(64), in first_name varchar(64), in type varchar(1), 
    in graduation_year year(4), in title enum('NA', 'Professor','Assistant Professor','Associate Professor','Adjunct Professor'))

BEGIN

    DECLARE new_uni varchar(12);
    
    SET new_uni = generate_uni(first_name, last_name);
    
    IF type = 'S' THEN INSERT INTO student VALUES (new_uni, last_name, first_name, type, graduation_year, 'NA');
    
    ELSEIF type = 'F' THEN INSERT INTO faculty VALUES (new_uni, last_name, first_name, type, 'NA', title);

END