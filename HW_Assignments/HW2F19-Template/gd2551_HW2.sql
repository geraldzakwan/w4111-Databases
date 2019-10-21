/*
  Name  : Geraldi Dzakwan
  UNI   : gd2551
*/

DROP TABLE IF EXISTS JOHNS;
DROP VIEW IF EXISTS AverageHeightWeight, AverageHeight;

/*QUESTION 0
EXAMPLE QUESTION
What is the highest salary in baseball history?
*/
SELECT 1
;
/*SAMPLE ANSWER*/
SELECT MAX(salary) as maxSalary
FROM Salaries;

/*QUESTION 1
Select the first name, last name, and given name of players who are taller than 6 ft
[hint]: Use "People"
*/
/* Explanation:
1. Using the fact that 1 ft equals 12 inches, we can have a where clause
of height > 12 * 6
2. Because height is a text, it is safer to cast it as UNSIGNED integer first
before making the comparison
*/
SELECT nameFirst, nameLast, nameGiven FROM People WHERE CAST(height AS UNSIGNED) > (12 * 6)
;

/*QUESTION 2
Create a Table of all the distinct players with a first name of John who were born in the United States and
played at Fordham university
Include their first name, last name, playerID, and birth state
Add a column called nameFull that is a concatenated version of first and last
[hint] Use a Join between People and CollegePlaying
*/
/* Explanation:
1. To make JOIN more efficient (less rows involved), I first filter the People and CollegePlaying table based
on the desired criteria (WHERE nameFirst = 'John' and birthCountry = 'USA') and (WHERE schoolID = 'fordham')
2. t1 and t2 are aliases for filtered People and CollegePlaying respectively, needed to avoid syntax error
3. DISTINCT is used to remove duplicate primaryID from CollegePlaying
The table CollegePlaying contains multiple entries of the same player (playerID) for different yearIDs.
4. nameFull is built using MySQL CONCAT, space is included between first and last name
If either of the nameFirst or nameLast is NULL, then return the one that is not NULL, hence the CASE condition
Notice that if both are NULL, CONCAT(nameFirst, ' ', nameLast) is also NULL. Thus, we only need 3 CASE conditions instead of 4
But then I realize this is not needed since all Johns have a non-NULL first and last name lol. But I just keep this as is
Reference: https://piazza.com/class/jy3jm0i73f8584?cid=391
5. There are only 2 rows in the resulting JOHNS table.
*/
CREATE Table JOHNS
  SELECT nameFirst, nameLast, t1.playerID, birthState,
  CASE
    WHEN nameFirst IS NOT NULL and nameLast IS NULL THEN nameFirst
    WHEN nameFirst IS NULL and nameLast IS NOT NULL THEN nameLast
    ELSE CONCAT(nameFirst, ' ', nameLast)
  END AS nameFull
  FROM
  	(SELECT nameFirst, nameLast, playerID, birthState FROM People WHERE nameFirst = 'John' AND birthCountry = 'USA') AS t1
  INNER JOIN
  	(SELECT DISTINCT playerID FROM CollegePlaying WHERE schoolID = 'fordham') AS t2
  ON t1.playerID = t2.playerID
;

/*QUESTION 3
Delete all Johns from the above table whose total career runs batted in is less than 2
[hint] use a subquery to select these johns from people by playerid
[hint] you may have to set sql_safe_updates = 1 to delete without a key
*/
/* Explanation:
1. Using SET SQL_SAFE_UPDATES = 0 to enable deleting without a key (I think the hint is wrong)
Then, set it back to 1 after this query is done.
2. Since JOHNS table includes playerID, I'm not sure why the hint suggests to use People table.
I can simply use a subquery to find playerID from Batting whose total RBI is less than two.
This is done using GROUP BY playerID and HAVING SUM(RBI) < 2 clause.
3. Use IN operator to check whether there are "johns" to be deleted, i.e.
whether there are playerID in Johns table that are in the subquery result
4. SUM aggregate functions will ignore RBI that is NULL and this is okay based on reference below
Reference: https://piazza.com/class/jy3jm0i73f8584?cid=437
5. 1 row is deleted in this case
*/
SET SQL_SAFE_UPDATES = 0;
DELETE FROM JOHNS WHERE playerID IN (
  SELECT playerID FROM Batting
  GROUP BY playerID
  HAVING SUM(RBI) < 2
);
SET SQL_SAFE_UPDATES = 1;

/*QUESTION 4
Group together players with the same birth year, and report the year,
 the number of players in the year, and average height for the year
 Order the resulting by year in descending order. Put this in a view
 [hint] height will be NULL for some of these years
*/
/* Explanation:
1. Exclude NULL birthYear by HAVING birthYear IS NOT NULL clause after GROUP BY
2. Since height is a text column, I make assumptions on player with empty string -> ' ' as height:
  a. The player will still be included in the total number of players using COUNT(playerID)
  b. The height will be treated like NULL, which means:
    - It doesn't count toward the averageHeight, for example average(2, 4, NULL) is 3 instead of 2
    - If all heights are NULL for some of the years, then averageHeight will also be NULL
    Those above are achieved by converting empty string to NULL using NULLIF(height, '')
3. If height is not an empty string, MySQL will automatically convert the string to a number
So, I don't handle that part and let it be handled by the default behavior of MySQL
4. Finally, ORDER BY birthYear DESC
*/
CREATE VIEW AverageHeight(birthYear, playerCount, averageHeight)
AS
  SELECT birthYear, COUNT(playerID), AVG(NULLIF(height, '')) FROM People
  GROUP BY birthYear
  HAVING birthYear IS NOT NULL
  ORDER BY birthYear DESC
;

/*QUESTION 5
Using Question 4, only include groups with an average weight >180 lbs,
also return the average weight of the group. This time, order by ascending
*/
/* Explanation:
1. Instead of computing both weight and height average, the more efficient approach
would be to use the AverageHeight view so that we don't recompute the AVG(NULLIF(height, ''))
2. Just like what I did in number 2, to make JOIN more efficient (less rows involved),
I first filter the People table based on the desired criteria: HAVING birthYear IS NOT NULL AND averageWeight > 180
3. t1 and t2 are aliases for AverageHeight and filtered People respectively, needed to avoid syntax error
4. Finally, ORDER BY birthYear ASC
*/
CREATE VIEW AverageHeightWeight(birthYear, playerCount, averageHeight, averageWeight)
AS
  SELECT t1.birthYear, playerCount, averageHeight, t2.averageWeight
  FROM
    (SELECT birthYear, playerCount, averageHeight FROM AverageHeight) AS t1
  INNER JOIN
    (
      SELECT birthYear, AVG(weight) AS averageWeight FROM People
      GROUP BY birthYear
      HAVING birthYear IS NOT NULL AND averageWeight > 180
    ) AS T2
  ON t1.birthYear = t2.birthYear
  ORDER BY birthYear ASC
;

-- select * from schools where state = 'NY';
/*QUESTION 6
Find the players who made it into the hall of fame who played for a college located in NY
return the player ID, first name, last name, and school ID. Order the players by School alphabetically.
Update all entries with full name Columbia University to 'Columbia University!' in the schools table
*/
SELECT playerID, nameFirst, nameLast, t3.schoolID, t3.name_full
FROM
  (
    SELECT t1.playerID, nameFirst, nameLast, schoolID
    FROM
      (
        /* Get players who made it into hall of fame */
        (SELECT playerID, nameFirst, nameLast FROM People WHERE playerID IN (
          SELECT DISTINCT playerID FROM HallofFame
        )) AS t1
      INNER JOIN
        /*
          Get unique combination of player and his school from CollegePlaying
          Notice that a player can have more than one school
        */
        (SELECT DISTINCT playerID, schoolID FROM CollegePlaying) AS t2
        ON t1.playerID = t2.playerID
      )
  ) AS t12
INNER JOIN
  (SELECT schoolID, name_full FROM Schools WHERE state = 'NY') AS t3
ON t12.schoolID = t3.schoolID
ORDER BY name_full ASC
;

/* Explanation:
1. Disabling SQL_SAFE_UPDATES (in order to update by column other than primary key),
i.e. SET name_full based on the condition: name_full = 'Columbia University!'
2. Since no primary key is specified for table Schools, then we can't update by
primary key and it also means that in the future there might be other
rows with different schoolID but same name: 'Columbia University'. Hence,
the best way to do the update is to condition based on name_full = 'Columbia University!'
*/
SET SQL_SAFE_UPDATES = 0;
UPDATE Schools SET name_full = 'Columbia University!' WHERE name_full = 'Columbia University';
SET SQL_SAFE_UPDATES = 1;

/*QUESTION 7
Find the team id, yearid and average HBP for each team using a subquery.
Limit the total number of entries returned to 100
group the entries by team and year and order by descending values of HBP
[hint] be careful to only include entries where AB is > 0
*/
/*
  Version 1: Averaging HBP per team per year. There is only one HBP
  per team per year as teamID and yearID are the primary key of Teams table
  So, this kinda doesn't make sense for me as we average over 1 value only.
  But, this is my official answer.
*/
SELECT teamID, yearID, AVG(NULLIF(HBP, '')) as averageHBP
FROM
  (SELECT teamID, yearID, HBP FROM Teams WHERE AB > 0) AS t1
GROUP BY teamID, yearID
ORDER BY averageHBP DESC
LIMIT 100
;

/*
  Version 2: Averaging HBP per team. There can be multiple HBPs
  per team as teams can play for many different years so we can
  take the average over the years.
  So, this kinda makes more sense for me even though
  my official answer is the Version 1.
*/
/*
SELECT teamID, AVG(NULLIF(HBP, '')) as averageHBP
FROM
  (SELECT teamID, HBP FROM Teams WHERE AB > 0) AS t1
GROUP BY teamID
ORDER BY averageHBP DESC
LIMIT 100
;
