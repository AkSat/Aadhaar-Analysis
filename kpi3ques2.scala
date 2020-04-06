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
 * 2. Find top 3 private agencies generating the most number of Aadhar cards?
 * 
 */


object kpi3ques2 extends App{
  
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
  
  println("=>  Using dataframe operations -------------")
  println("---------------- Top 3 private agencies generating most number of Aadhaar cards -------------")
  df.select("privateAgency","generated").groupBy("privateAgency").agg(sum("generated").alias("total")).orderBy(desc("total")).take(3).foreach(println)
   
  println("\n\n=>  Using spark sql -------------")
  println("---------------- Top 3 private agencies generating most number of Aadhaar cards -------------")
  df.createOrReplaceTempView("aadhaar")
  session.sql("select privateAgency,sum(generated) as total from aadhaar group by privateAgency order by total desc limit 3").show()
 
  /*
   * 
   * Output
   * =>  Using dataframe operations -------------
    ---------------- Top 3 private agencies generating most number of Aadhaar cards -------------
    [Wipro Ltd,745751]
    [Vakrangee Softwares Limited,225273]
    [Swathy Smartcards Hi-Tech Pvt,211790]
    
    
    =>  Using spark sql -------------
    ---------------- Top 3 private agencies generating most number of Aadhaar cards -------------
    +--------------------+------+
    |       privateAgency| total|
    +--------------------+------+
    |           Wipro Ltd|745751|
    |Vakrangee Softwar...|225273|
    |Swathy Smartcards...|211790|
    +--------------------+------+

   * 
   * 
   */
  
}