import scala.collection.mutable.WrappedArray
import spark.implicits._
import org.apache.spark.sql.functions._

val business = spark.read.json("../home/data/business.json")
val review = spark.read.json("../home/data/review.json")
 
val b = business.withColumn("category", explode(when(col("categories").isNotNull, col("categories")).otherwise(array(lit(null).cast("string")))))

b.registerTempTable("business")
review.registerTempTable("review")

%sql 
SELECT b.city, b.category, COUNT(DISTINCT r.user_id) AS total_reviewers FROM review r INNER JOIN business b  ON r.business_id = b.business_id  
WHERE state	==	'AL' 	OR 
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

GROUP BY b.city,b.category ORDER BY b.city