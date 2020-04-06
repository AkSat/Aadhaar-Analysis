package com.aadhaar.dataframe
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.catalyst.plans.logical.Distinct



/*
 * 5. The summary of the acceptance percentage of all the Aadhaar cards applications by bucketing the age group into 10 buckets.
 * 
 */


object kpi5ques5 extends App{
  
  Logger.getRootLogger().setLevel(Level.ERROR)
  Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
  
  val session = SparkSession.builder().appName("aadhaar").master("local").getOrCreate()
  
  val schema1 = new StructType()
                .add("date",StringType,true)
                .add("registrar",StringType,true)
                .add("privateAgency",StringType,true)
                .add("state",StringType,true)
                .add("district",StringType,true)
                .add("subdistrict",StringType,true)
                .add("pincode",StringType,true)
                .add("gender",StringType,true)
                .add("age",IntegerType,true)
                .add("generated",IntegerType,true)
                .add("rejected",IntegerType,true)
                .add("mobileno",IntegerType,true)
                .add("emailid",IntegerType,true)
                
 
  val csv1 = session.sparkContext.textFile("F:/DataFlair - Spark and Scala/LMS Downloads/Projects/aadhaar_data.csv")
  
  val df1 = csv1.map(f => {
    val arr = f.split(",")
    Row(arr(0),arr(1),arr(2),arr(3),arr(4),arr(5),arr(6),arr(7),arr(8).toInt,arr(9).toInt,arr(10).toInt,arr(11).toInt,arr(12).toInt)
  })
  
  val df = session.createDataFrame(df1, schema1)
 
  
  println("=> Using dataframe operations -------------")
  
  val totalApplicants = df.select((col("generated") + col("rejected")).alias("enrolled")).agg(sum("enrolled")).first().get(0)
  
  val minage = df.select("age").agg(min("age")).rdd.first().get(0)
  val maxage = df.select("age").agg(max("age")).rdd.first().get(0)
  

  
  var interval = (maxage.toString().toInt - minage.toString().toInt)/10 
  interval = interval + 1
  df
  .withColumn("range", col("age") - (col("age") % interval))
  .withColumn("range", concat(col("range"), lit(" - "), col("range") + interval))
  .withColumn("accepted", col("generated") - col("rejected"))
  .groupBy("range").agg(bround(sum("accepted")/totalApplicants*100,7).alias("PercAccepted"))
  .orderBy(desc("PercAccepted")).show()
  
  
 
  
  /*
   * 
   * Output :
   * => Using dataframe operations -------------
    +---------+------------+
    |    range|PercAccepted|
    +---------+------------+
    |  19 - 38|  30.2588108|
    |   0 - 19|  21.2673593|
    |  38 - 57|  20.1526398|
    |  57 - 76|   9.0185774|
    |  76 - 95|   0.8577339|
    | 95 - 114|   0.0100137|
    |114 - 133|    4.056E-4|
    |152 - 171|    1.268E-4|
    |133 - 152|     5.07E-5|
    |171 - 190|     2.54E-5|
    +---------+------------+

   * 
   
  
   * 
   */
  
  
}