import org.apache.spark.sql.functions._
import spark.implicits._

val spark = new org.apache.spark.sql.SQLContext(sc)

val business_raw_data = spark.read.json("../home/data/business.json")
val review_data = spark.read.json("../home/data/review.json")

val bus = business_raw_data.withColumn("category", explode(when(col("categories").isNotNull, col("categories")).otherwise(array(lit(null).cast("string")))))
    
bus.registerTempTable("business")

val filter_1 = spark.sql("SELECT b.business_id,b.name,b.categories,b.stars FROM business b WHERE ACOS(SIN((55.9469753*3.14)/180)*SIN((latitude*3.14)/180) + COS((55.9469753*3.14)/180)*COS((latitude*3.14)/180)*COS(((-3.2096308 - longitude)*3.14)/180))*6371 < 15 AND b.category == 'Food'");

val flattener = filter_1.withColumn("category", explode(when(col("categories").isNotNull, col("categories")).otherwise(array(lit(null).cast("string")))))
flattener.registerTempTable("flattener")	
	
val top_stars = spark.sql("SELECT b.business_id,b.name,b.category,b.stars FROM flattener b ORDER BY b.stars DESC LIMIT 10")
top_stars.registerTempTable("top_stars")

val bottom_stars = spark.sql("SELECT f.business_id,f.name,f.category,f.stars from flattener f ORDER BY f.stars ASC LIMIT 10")
bottom_stars.registerTempTable("bottom_stars")

review_data.registerTempTable("review")

val filter_date = spark.sql("SELECT r.business_id,r.date,r.stars FROM review r WHERE month(date)>=1 AND month(date)<=5")
filter_date.registerTempTable("filter_date")

val joiner_top = spark.sql("SELECT t.name,t.category, t.business_id, f.date, f.stars FROM top_stars t INNER JOIN filter_date f on t.business_id = f.business_id")
joiner_top.registerTempTable("joiner_top")

val joiner_bottom = spark.sql("SELECT b.name, b.category, b.business_id, f.date, f.stars FROM bottom_stars b INNER JOIN filter_date f on b.business_id = f.business_id")
joiner_bottom.registerTempTable("joiner_bottom")

val join_total = spark.sql("SELECT * FROM joiner_top AS top_all UNION SELECT * FROM joiner_bottom AS bottom_all")
join_total.registerTempTable("join_total")

%sql 
SELECT j.business_id, avg(stars) AS average_stars FROM join_total j GROUP BY j.business_id