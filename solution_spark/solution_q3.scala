import scala.collection.mutable.WrappedArray
import spark.implicits._
import org.apache.spark.sql.functions._

val business = spark.read.json("../home/data/business.json")
 
val b = business.withColumn("category", explode(when(col("categories").isNotNull, col("categories")).otherwise(array(lit(null).cast("string")))))
    
b.registerTempTable("business")

%sql 

SELECT b.category, AVG(stars) as average_stars from business b WHERE ACOS(SIN((55.9469753*3.14)/180)*SIN((latitude*3.14)/180) + COS((55.9469753*3.14)/180)*COS((latitude*3.14)/180)*COS(((-3.2096308 - longitude)*3.14)/180))*6371 < 15  GROUP BY b.category ORDER BY b.category