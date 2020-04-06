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
 * 3. Find the number of Aadhaar registrations rejected in Uttar Pradesh and Maharashtra?
 * 
 */


object kpi4ques3 extends App{
  
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
  print("---------------- Number of aadhaar registrations rejected in Uttar Pradesh and Maharashtra -------------")  
 df.select("state","rejected").filter(col("state") === "Maharashtra" || col("state") === "Uttar Pradesh")
   .groupBy("state").agg(sum("rejected").as("TotalRejected")).show()
  
   
  println("\n=>  Using spark sql -------------")
  println("---------------- Number of aadhaar registrations rejected in Uttar Pradesh and Maharashtra -------------")
  
  df.createOrReplaceTempView("aadhaar")
  session.sql("select state,sum(rejected) as TotalRejected from aadhaar group by state having state = 'Maharashtra' OR state = 'Uttar Pradesh'").show()
 
  /*
   * 
   * Output :
   * 
   * => Using dataframe operations -------------
    ---------------- Number of aadhaar registrations rejected in Uttar Pradesh and Maharashtra -------------+-------------+-------------+
    |        state|TotalRejected|
    +-------------+-------------+
    |  Maharashtra|        45704|
    |Uttar Pradesh|        24752|
    +-------------+-------------+
    
    
    =>  Using spark sql -------------
    ---------------- Number of aadhaar registrations rejected in Uttar Pradesh and Maharashtra -------------
    +-------------+-------------+
    |        state|TotalRejected|
    +-------------+-------------+
    |  Maharashtra|        45704|
    |Uttar Pradesh|        24752|
    +-------------+-------------+

   * 
   */
  
  
}