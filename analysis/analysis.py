from sqlalchemy.engine import Engine
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

load_dotenv()
POSTGRES_USER = os.environ["POSTGRES_USER"]
POSTGRES_DB = os.environ["POSTGRES_DB"]
POSTGRES_PASSWORD = os.environ["POSTGRES_PASSWORD"]
POSTGRES_PORT = os.environ["POSTGRES_PORT"]
POSTGRES_SERVER = os.environ["POSTGRES_SERVER"]
ENGINE = create_engine(f'postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_SERVER}:{POSTGRES_PORT}/{POSTGRES_DB}')

queries = []

top_regions_query = f"""
SELECT
    regions."REGION"
    , COUNT(athletes."ATHLETEID") AS count_athletes
FROM
    athletes
LEFT JOIN regions ON regions."NOC" = athletes."NOC"
GROUP BY regions."REGION"
ORDER BY count_athletes DESC
LIMIT 10
"""

queries.append(("Top 10 regions by count of athletes:",top_regions_query))

bottom_regions_query = f"""
SELECT
    regions."REGION"
    , COUNT(athletes."ATHLETEID") AS count_athletes
FROM
    athletes
LEFT JOIN regions ON regions."NOC" = athletes."NOC"
GROUP BY regions."REGION"
ORDER BY count_athletes ASC
LIMIT 10
"""

queries.append(("Bottom 10 regions by count of athletes:",bottom_regions_query))

regions_dist_query = f"""
WITH region_counts AS (
	SELECT
	    regions."REGION"
	    , COUNT(athletes."ATHLETEID") AS count_athletes
	FROM
	    athletes
	LEFT JOIN regions ON regions."NOC" = athletes."NOC"
	GROUP BY regions."REGION"
	ORDER BY count_athletes DESC
)

SELECT
	CEIL(AVG(count_athletes)) AS mean_athlete_count
	, MIN(count_athletes) AS min_athlete_count
	, CEIL(PERCENTILE_CONT( 0.25) WITHIN GROUP (ORDER BY count_athletes ASC)) AS p25_athlete_count
	, CEIL(PERCENTILE_CONT( 0.5) WITHIN GROUP (ORDER BY count_athletes ASC)) AS median_athlete_count
	, CEIL(PERCENTILE_CONT( 0.75) WITHIN GROUP (ORDER BY count_athletes ASC)) AS p75_athlete_count
	, MAX(count_athletes) AS max_athlete_count
FROM region_counts
"""

queries.append(("Distribution of regions by count of athletes:", regions_dist_query))


sex_dist_query = f"""
SELECT
    athletes."SEX"
    , COUNT(athletes."ATHLETEID") AS count_athletes
FROM
    athletes
GROUP BY athletes."SEX"
ORDER BY count_athletes DESC
"""

queries.append(("Distribution of sex by count of athletes:", sex_dist_query))

decade_medals_query = f"""
WITH team_medals AS (
	SELECT
		athletes."TEAM"
		, ("YEAR" / 10)*10 AS "DECADE"
		, event_results."EVENTRESULTID"
	FROM event_results
	LEFT JOIN athletes ON 
		event_results."ATHLETEID" = athletes."ATHLETEID"
	WHERE event_results."MEDAL" IS NOT NULL
)

, medal_placings AS (
	SELECT
		"TEAM"
		, "DECADE"
		, DENSE_RANK() OVER(
			PARTITION BY "DECADE"
			ORDER BY COUNT("EVENTRESULTID") DESC
			) AS "DECADE_PLACE"
	FROM team_medals
	GROUP BY "TEAM", "DECADE"
)

SELECT * FROM medal_placings
WHERE "DECADE_PLACE" = 1
ORDER BY "DECADE" DESC
"""

queries.append(("Team with the most medals per decade:", decade_medals_query))


top_3_cities_query = f"""
SELECT
	"CITY"
	, COUNT(DISTINCT("YEAR","SEASON")) AS olympics_count
FROM event_results
GROUP BY "CITY"
ORDER BY olympics_count DESC
LIMIT 3
"""

queries.append(("Top 3 cities to host Olympics again:", top_3_cities_query))


for description, query in queries:
    columns = ENGINE.execute(query).keys()
    results = ENGINE.execute(query).fetchall()
    print(description)
    print(f"Columns: {columns}")
    print(f"Results: {results}")
    print('\n')
