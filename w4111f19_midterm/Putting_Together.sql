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
  `teamID` varchar(3),
  `lgID` varchar(2),
  `playerID` varchar(16),
  `salary` unsigned,
  PRIMARY KEY (playerID, teamID, yearID, lgID),
  FOREIGN KEY (playerID, teamID, yearID) REFERENCES Appearances(playerID, teamID, yearID)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- SELECT teamID, yearID FROM Teams LIMIT 50;

-- DESC appearances;

-- SELECT DISTINCT LENGTH(playerID) FROM People;