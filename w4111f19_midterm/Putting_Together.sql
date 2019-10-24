use lahman2019clean;

-- select teamID from salaries where length(teamID) > 3;

-- DESC people;

-- SELECT playerID, teamID, yearID, COUNT(1) 
-- FROM salaries 
-- GROUP BY playerID, teamID, yearID 
-- HAVING COUNT(1) > 1;

-- SELECT playerID, teamID, yearID, lgID FROM salaries;

DROP TABLE IF EXISTS salaries_clean;

CREATE TABLE `salaries_clean` (
   `yearID` varchar(4),
   `teamID` varchar(4),
   `lgID` varchar(2),
   `playerID` varchar(16),
   `salary` int unsigned,
   PRIMARY KEY (playerID, teamID, yearID, lgID),
   -- FOREIGN KEY (playerID, teamID, yearID, lgID) REFERENCES Appearances(playerID, teamID, yearID, lgID)
   FOREIGN KEY (playerID) REFERENCES People(playerID),
   FOREIGN KEY (teamID, yearID) REFERENCES Teams(teamID, yearID)
   -- FOREIGN KEY (teamID, yearID, lgID) REFERENCES Teams(teamID, yearID, lgID)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

INSERT INTO salaries_clean
SELECT * FROM salaries;

-- ALTER TABLE Teams 
-- MODIFY lgID varchar(2);

/* 
SELECT playerID, teamID, yearID, salary FROM salaries WHERE NOT EXISTS (
	SELECT playerID, teamID, yearID FROM appearances
	WHERE salaries.playerID = appearances.playerID AND salaries.teamID = appearances.teamID AND salaries.yearID = appearances.yearID
);
*/

-- SELECT teamID, yearID FROM Teams LIMIT 50;

-- DESC appearances;

-- SELECT playerID FROM appearances WHERE G_all IS NULL;

-- SELECT COUNT(*) FROM appearances;

-- SELECT DISTINCT LENGTH(playerID) FROM People;

