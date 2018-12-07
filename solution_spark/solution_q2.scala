import scala.collection.mutable.WrappedArray
import spark.implicits._
import org.apache.spark.sql.functions._

val business = spark.read.json("../home/data/business.json")
 
val b = business.withColumn("category", explode(when(col("categories").isNotNull, col("categories")).otherwise(array(lit(null).cast("string")))))
b.registerTempTable("business")

%sql 
SELECT b.category , b.city, count(stars) as total_stars from business b GROUP BY b.category, b.city ORDER BY b.category ASC