REGISTER ../home/jar-files/json-simple-1.1.1.jar;
REGISTER ../home/jar-files/elephant-bird-pig-4.15.jar;
REGISTER ../home/jar-files/elephant-bird-hadoop-compat-4.15.jar;
REGISTER ../home/jar-files/elephant-bird-core-4.15.jar;
REGISTER ../home/jar-files/piggybank.jar;
DEFINE CSVExcelStorage org.apache.pig.piggybank.storage.CSVExcelStorage;
DEFINE ElephantBird com.twitter.elephantbird.pig.load.JsonLoader('-nestedLoad');

business = LOAD '../home/data/business.json' using ElephantBird() as (json:map[]);
reviewers = LOAD '../home/data/review.json' using ElephantBird() as (json:map[]);

businesses = FOREACH business GENERATE (int)json#'review_count' AS review_count, json#'city' AS city, FLATTEN(json#'categories') as categories, json#'business_id' as b_id, json#'state' AS state;
attr_review = FOREACH reviewers GENERATE json#'business_id' as business_id, json#'user_id' AS user_id;

filter_state = FILTER businesses BY 
state	==	'AL' 	OR 
state 	==	'AK'	OR
state	==	'AZ'	OR
state	==	'AR'	OR
state	==	'CA'	OR
state	==	'CO'	OR
state	==	'CT'	OR
state	==	'DE'	OR
state	==	'FL'	OR
state	==	'GA'	OR
state	==	'HI'	OR
state	==	'ID'	OR
state	==	'IL'	OR
state	==	'IN'	OR
state	==	'IA'	OR
state	==	'KS'	OR
state	==	'KY'	OR
state	==	'LA'	OR
state	==	'ME'	OR
state	==	'MD'	OR
state	==	'MA'	OR
state	==	'MI'	OR
state	==	'MN'	OR
state	==	'MS'	OR
state	==	'MO'	OR
state	==	'MT'	OR
state	==	'NE'	OR
state	==	'NV'	OR
state	==	'NH'	OR
state	==	'NJ'	OR
state	==	'NM'	OR
state	==	'NY'	OR
state	==	'NC'	OR
state	==	'ND'	OR
state	==	'OH'	OR
state	==	'OK'	OR
state	==	'OR'	OR
state	==	'PA'	OR
state	==	'RI'	OR
state	==	'SC'	OR
state	==	'SD'	OR
state	==	'TN'	OR
state	==	'TX'	OR
state	==	'UT'	OR
state	==	'VT'	OR
state	==	'VA'	OR
state	==	'WA'	OR
state	==	'WV'	OR
state	==	'WI'	OR
state	==	'WY';

attr = JOIN filter_state BY b_id, attr_review BY business_id;

groupBy = GROUP attr BY (city, categories);

count_review = FOREACH groupBy GENERATE FLATTEN(group) as (city, categories), COUNT(attr.user_id) as total_count;


orderBy = ORDER count_review BY city;

STORE orderBy INTO '../home/solutions/solution-1.csv' USING CSVExcelStorage() PARALLEL 1;