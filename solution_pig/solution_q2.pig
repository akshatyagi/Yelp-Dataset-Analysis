REGISTER ../home/jar-files/json-simple-1.1.1.jar;
REGISTER ../home/jar-files/elephant-bird-pig-4.15.jar;
REGISTER ../home/jar-files/elephant-bird-hadoop-compat-4.15.jar;
REGISTER ../home/jar-files/elephant-bird-core-4.15.jar;
REGISTER ../home/jar-files/piggybank.jar;
DEFINE CSVExcelStorage org.apache.pig.piggybank.storage.CSVExcelStorage;
DEFINE ElephantBird com.twitter.elephantbird.pig.load.JsonLoader('-nestedLoad');


business = LOAD '../home/data/business.json' using ElephantBird() as (json:map[]);
attributes = FOREACH business GENERATE (float)json#'stars' AS stars, json#'city' AS city, FLATTEN(json#'categories') as categories;
groupBy = GROUP attributes BY (city,categories);
total_stars = FOREACH groupBy GENERATE COUNT(attributes.stars) as count_stars, FLATTEN(group) as (city,categories);
result = ORDER total_stars by count_stars DESC;

STORE result INTO '../home/solutions/solution-2.csv' USING CSVExcelStorage() PARALLEL 1;