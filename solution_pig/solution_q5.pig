REGISTER ../home/jar-files/json-simple-1.1.1.jar;
REGISTER ../home/jar-files/elephant-bird-pig-4.15.jar;
REGISTER ../home/jar-files/elephant-bird-hadoop-compat-4.15.jar;
REGISTER ../home/jar-files/elephant-bird-core-4.15.jar;
REGISTER ../home/jar-files/piggybank.jar;
DEFINE CSVExcelStorage org.apache.pig.piggybank.storage.CSVExcelStorage;
DEFINE ElephantBird com.twitter.elephantbird.pig.load.JsonLoader('-nestedLoad');

business = LOAD '../home/data/business.json' using ElephantBird() as (json:map[]);
review = LOAD '../home/data/review.json' using ElephantBird() as (json:map[]);


attributes = FOREACH business GENERATE (float)json#'stars' AS stars,FLATTEN(json#'categories') as categories, (double)json#'latitude' AS latitude, (double)json#'longitude' AS longitude, json#'business_id' AS bid;

filter_1 = filter attributes by ACOS(SIN((55.9469753*3.14)/180)*SIN((latitude*3.14)/180) + COS((55.9469753*3.14)/180)*COS((latitude*3.14)/180)*COS(((-3.2096308 - longitude)*3.14)/180))*6371 < 15 AND (categories == 'Food');

top_filter = ORDER filter_1 BY stars DESC;
top_data = LIMIT top_filter 10;

bottom_filter = ORDER filter_1 BY stars ASC;
bottom_data = LIMIT bottom_filter 10;

unionize = UNION top_data, bottom_data;

rev = FOREACH review GENERATE json#'business_id' AS business_id,(datetime)json#'date' AS date,
json#'review_id' AS review_id, (float)json#'stars' AS stars, json#'user_id' AS user_id;

filter_2 = FILTER rev BY (GetMonth(date)==1) OR (GetMonth(date)==2) OR(GetMonth(date)==3) OR (GetMonth(date)==4) OR (GetMonth(date)==5);

combined_result = JOIN unionize BY bid, filter_2 BY business_id;

result = FOREACH combined_result GENERATE filter_2::stars, filter_2::business_id,filter_2::review_id;

grouper = GROUP result by filter_2::business_id;

final = FOREACH grouper GENERATE group as bus_id, AVG(result.stars) as star;

STORE final INTO '../home/solutions/solution-5.csv' USING CSVExcelStorage() PARALLEL 1;