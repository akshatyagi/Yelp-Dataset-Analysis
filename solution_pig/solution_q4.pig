REGISTER ../home/jar-files/json-simple-1.1.1.jar;
REGISTER ../home/jar-files/elephant-bird-pig-4.15.jar;
REGISTER ../home/jar-files/elephant-bird-hadoop-compat-4.15.jar;
REGISTER ../home/jar-files/elephant-bird-core-4.15.jar;
REGISTER ../home/jar-files/piggybank.jar;
DEFINE CSVExcelStorage org.apache.pig.piggybank.storage.CSVExcelStorage;
DEFINE ElephantBird com.twitter.elephantbird.pig.load.JsonLoader('-nestedLoad');

business = LOAD '../home/data/business.json' using ElephantBird() as (json:map[]);
review = LOAD '../home/data/review.json' using ElephantBird() as (json:map[]);
user = LOAD '../home/data/user.json' using ElephantBird() as (json:map[]);

attr_business = FOREACH business GENERATE json#'business_id' AS bid, FLATTEN (json#'categories') as categories,(double)json#'latitude' AS latitude, (double)json#'longitude' AS longitude;
attr_user = FOREACH user GENERATE json#'user_id' AS user_id, (int)json#'review_count' as review_count;
attr_review = FOREACH review GENERATE json#'user_id' AS user_id, json#'review_id' as review_id, json#'business_id' as business_id, (float)json#'stars' AS star;

ranker = ORDER attr_user BY review_count DESC;
limiter = LIMIT ranker 10;
joiner_1 = JOIN attr_review BY business_id, attr_business BY bid;
joiner_2 = JOIN joiner_1 BY attr_review::user_id, limiter BY user_id;

filter_1 = FILTER joiner_2 BY ACOS(SIN((55.9469753*3.14)/180)*SIN((attr_business::latitude*3.14)/180) + COS((55.9469753*3.14)/180)*COS((attr_business::latitude*3.14)/180)*COS(((-3.2096308 - attr_business::longitude)*3.14)/180))*6371 < 15;

getData = FOREACH filter_1 GENERATE attr_review::user_id, attr_business::categories, attr_review::star;
grouper = GROUP getData by (attr_review::user_id, attr_business::categories);
average_stars = FOREACH grouper GENERATE FLATTEN(group) as (usr, cato), AVG(getData.star) as star;

STORE average_stars INTO '../home/solutions/solution-4.csv' USING CSVExcelStorage() PARALLEL 1;