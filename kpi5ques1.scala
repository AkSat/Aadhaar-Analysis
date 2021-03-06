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
 * 1. The top 3 states where the percentage of Aadhaar cards being generated for males is the highest
 * 
 */


object kpi5ques1 extends App{
  
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
  
  print("---------------- top 3 states where the percentage of Aadhaar cards being generated for males is the highest -------------\n")  
  df.where("gender = 'M'").groupBy("state").agg(bround(sum("generated")/totalApplicants * 100,7).as("PercGenerated"))
    .orderBy(desc("PercGenerated")).take(3).foreach(println)
  
   
  println("\n=>  Using spark sql -------------")
  println("---------------- top 3 states where the percentage of Aadhaar cards being generated for males is the highest -------------")
  
  df.createOrReplaceTempView("aadhaar")
  
  session.sql(s"select state,(sum(generated)/$totalApplicants * 100) as PercGenerated from aadhaar where gender = 'M' group by state order by PercGenerated desc limit 3").show()
 
  /*
   * 
   * Output :
   * 
  
    => Using dataframe operations -------------
    ---------------- top 3 states where the percentage of Aadhaar cards being generated for males is the highest -------------
    [Maharashtra,12.6273013]
    [Uttar Pradesh,4.8234033]
    [Andhra Pradesh,3.1126001]
    
    =>  Using spark sql -------------
    ---------------- top 3 states where the percentage of Aadhaar cards being generated for males is the highest -------------
    +--------------+------------------+
    |         state|     PercGenerated|
    +--------------+------------------+
    |   Maharashtra| 12.62730125894265|
    | Uttar Pradesh| 4.823403252057253|
    |Andhra Pradesh|3.1126000740256248|
    +--------------+------------------+




   * 
   */
  
  
}