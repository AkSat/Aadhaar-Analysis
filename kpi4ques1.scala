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
 * 1. Write a command to see the correlation between “age” and “mobile_number”? (Hint: Consider
the percentage of people who have provided the mobile number out of the total applicants)
 * 
 */


object kpi4ques1 extends App{
  
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
  println("---------------- Percentage of people who have provided mobile number in the age range -------------")
  
  val totalApplicants = df.select((col("generated") + col("rejected")).alias("enrolled")).agg(sum("enrolled")).first().get(0)
  //println("Total Applicants : " + totalApplicants)
  
  val interval = 5
   df.withColumn("range", col("age") - (col("age") % interval))
  .withColumn("range", concat(col("range"), lit(" - "), col("range") + interval)) //optional one
  .groupBy("range").agg(bround(sum("mobileno")/totalApplicants,7).alias("Percentage")).orderBy(desc("Percentage")).show()
  
 
/*
 * 
 * Output :
 * 
 * => Using dataframe operations -------------
    ---------------- Percentage of people who have provided mobile number in the age range -------------
    +--------+----------+
    |   range|Percentage|
    +--------+----------+
    | 20 - 25| 0.0033066|
    | 25 - 30| 0.0027539|
    | 30 - 35| 0.0018068|
    | 15 - 20|  0.001657|
    | 35 - 40| 0.0011649|
    | 40 - 45|  7.973E-4|
    | 45 - 50|  5.894E-4|
    | 50 - 55|  4.611E-4|
    | 10 - 15|  3.661E-4|
    | 55 - 60|  3.349E-4|
    |  5 - 10|  3.108E-4|
    |   0 - 5|  2.697E-4|
    | 60 - 65|  2.375E-4|
    | 65 - 70|  1.339E-4|
    | 70 - 75|   7.15E-5|
    | 75 - 80|   4.08E-5|
    | 80 - 85|   1.34E-5|
    | 85 - 90|    6.1E-6|
    | 90 - 95|    1.5E-6|
    |95 - 100|    8.0E-7|
    +--------+----------+
    only showing top 20 rows

 * 
 * 
 */
  
}