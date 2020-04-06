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
 * 1. Find top 3 states generating most number of Aadhaar cards
 * 
 */


object kpi3ques1 extends App{
  
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
  println("---------------- Top 3 states generating most number of Aadhaar cards -------------")
  df.select("state","generated").groupBy("state").agg(sum("generated").alias("total")).orderBy(desc("total")).take(3).foreach(println)
  
  println("\n\n=>  Using spark sql -------------")
  println("---------------- Top 3 states generating most number of Aadhaar cards -------------")
  df.createOrReplaceTempView("aadhaar")
  session.sql("select state,sum(generated) as total from aadhaar group by state order by total desc limit 3").show()
 
  /*
   * Output:
   * =>  Using dataframe operations -------------
    ---------------- Top 3 states generating most number of Aadhaar cards -------------
    [Maharashtra,951201]
    [Uttar Pradesh,385463]
    [Andhra Pradesh,270055]
    
    
    =>  Using spark sql -------------
    ---------------- Top 3 states generating most number of Aadhaar cards -------------
    +--------------+------+
    |         state| total|
    +--------------+------+
    |   Maharashtra|951201|
    | Uttar Pradesh|385463|
    |Andhra Pradesh|270055|
    +--------------+------+

   * 
   * 
   */
}