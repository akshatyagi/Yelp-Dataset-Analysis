REGISTER ../home/jar-files/json-simple-1.1.1.jar;
REGISTER ../home/jar-files/elephant-bird-pig-4.15.jar;
REGISTER ../home/jar-files/elephant-bird-hadoop-compat-4.15.jar;
REGISTER ../home/jar-files/elephant-bird-core-4.15.jar;
REGISTER ../home/jar-files/piggybank.jar;
DEFINE CSVExcelStorage org.apache.pig.piggybank.storage.CSVExcelStorage;
DEFINE ElephantBird com.twitter.elephantbird.pig.load.JsonLoader('-nestedLoad');

businesses = LOAD '../home/data/business.json' using ElephantBird() as (json:map[]);
attributes = FOREACH businesses GENERATE (float)json#'stars' AS stars, FLATTEN(json#'categories') as categories, (double)json#'latitude' AS latitude, (double)json#'longitude' AS longitude;
filterlatlong = FILTER attributes BY ACOS(SIN((55.9469753*3.14)/180)*SIN((latitude*3.14)/180) + COS((55.9469753*3.14)/180)*COS((latitude*3.14)/180)*COS(((-3.2096308 - longitude)*3.14)/180))*6371 < 15;

flattener = FOREACH filterlatlong GENERATE stars, FLATTEN(categories);
grouper = GROUP flattener BY categories;
result = FOREACH grouper GENERATE FLATTEN(group) as (categories), AVG(flattener.stars) as avg_stars;
STORE result INTO '../home/solutions/solution-3.csv' USING CSVExcelStorage() PARALLEL 1;