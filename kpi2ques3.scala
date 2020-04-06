package com.aadhaar.dataframe
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.apache.log4j.Logger
import org.apache.log4j.Level



/*
 * 3. Find the number of males and females in each state from the table.
 * 
 */


object kpi2ques3 extends App{
  
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
  println("---------------- No. of males in each state -------------")
  // ---- the number of males in each state
  df.select("state","gender").filter(col("gender") === "M").groupBy("state").agg(count("gender").alias("NoOfMales")).sort(desc("NoOfMales")).show()
  
  println("---------------- No. of females in each state -------------")
  // ---- the number of females in each state
  df.select("state","gender").filter("gender == 'F'").groupBy("state").agg(count("gender").alias("NoOfFemales")).sort(desc("NoOfFemales")).show()
  
    
  println("\n\n=>  Using spark sql -------------")
  df.createOrReplaceTempView("aadhaar")
  println("---------------- No. of males in each state -------------")
  session.sql("select state,count(gender) as NoOfMales from aadhaar where gender == 'M' group by state order by NoOfMales desc").show(true)
  
  println("---------------- No. of females in each state -------------")
  session.sql("select state,count(gender) as NoOfFemales from aadhaar where gender == 'F' group by state order by NoOfFemales desc").show(true)
  
  /*
   * 
   * =>  Using dataframe operations -------------
    ---------------- No. of males in each state -------------
    +----------------+---------+
    |           state|NoOfMales|
    +----------------+---------+
    |     Maharashtra|   147255|
    |   Uttar Pradesh|   104209|
    |  Andhra Pradesh|    74748|
    |       Rajasthan|    68260|
    |           Bihar|    59173|
    |  Madhya Pradesh|    56623|
    |         Gujarat|    45506|
    |       Karnataka|    44916|
    |          Kerala|    41801|
    |       Jharkhand|    32005|
    |      Tamil Nadu|    28359|
    |          Punjab|    25494|
    |     West Bengal|    22154|
    |         Haryana|    21890|
    |           Delhi|    19397|
    |          Odisha|    12213|
    |    Chhattisgarh|    10189|
    |Himachal Pradesh|    10032|
    |     Uttarakhand|     6783|
    |         Manipur|     3651|
    +----------------+---------+
    only showing top 20 rows
    
    ---------------- No. of females in each state -------------
    +----------------+-----------+
    |           state|NoOfFemales|
    +----------------+-----------+
    |     Maharashtra|     138029|
    |   Uttar Pradesh|     105187|
    |  Andhra Pradesh|      80768|
    |       Rajasthan|      67928|
    |           Bihar|      54227|
    |  Madhya Pradesh|      53619|
    |       Karnataka|      44599|
    |          Kerala|      44411|
    |         Gujarat|      41656|
    |       Jharkhand|      31394|
    |         Haryana|      25035|
    |          Punjab|      23357|
    |     West Bengal|      20592|
    |           Delhi|      17516|
    |      Tamil Nadu|      16606|
    |    Chhattisgarh|      13349|
    |          Odisha|      10997|
    |Himachal Pradesh|       9103|
    |     Uttarakhand|       6813|
    |             Goa|       3352|
    +----------------+-----------+
    only showing top 20 rows
    
    
    
    =>  Using spark sql -------------
    ---------------- No. of males in each state -------------
    +----------------+---------+
    |           state|NoOfMales|
    +----------------+---------+
    |     Maharashtra|   147255|
    |   Uttar Pradesh|   104209|
    |  Andhra Pradesh|    74748|
    |       Rajasthan|    68260|
    |           Bihar|    59173|
    |  Madhya Pradesh|    56623|
    |         Gujarat|    45506|
    |       Karnataka|    44916|
    |          Kerala|    41801|
    |       Jharkhand|    32005|
    |      Tamil Nadu|    28359|
    |          Punjab|    25494|
    |     West Bengal|    22154|
    |         Haryana|    21890|
    |           Delhi|    19397|
    |          Odisha|    12213|
    |    Chhattisgarh|    10189|
    |Himachal Pradesh|    10032|
    |     Uttarakhand|     6783|
    |         Manipur|     3651|
    +----------------+---------+
    only showing top 20 rows
    
    ---------------- No. of females in each state -------------
    +----------------+-----------+
    |           state|NoOfFemales|
    +----------------+-----------+
    |     Maharashtra|     138029|
    |   Uttar Pradesh|     105187|
    |  Andhra Pradesh|      80768|
    |       Rajasthan|      67928|
    |           Bihar|      54227|
    |  Madhya Pradesh|      53619|
    |       Karnataka|      44599|
    |          Kerala|      44411|
    |         Gujarat|      41656|
    |       Jharkhand|      31394|
    |         Haryana|      25035|
    |          Punjab|      23357|
    |     West Bengal|      20592|
    |           Delhi|      17516|
    |      Tamil Nadu|      16606|
    |    Chhattisgarh|      13349|
    |          Odisha|      10997|
    |Himachal Pradesh|       9103|
    |     Uttarakhand|       6813|
    |             Goa|       3352|
    +----------------+-----------+
    only showing top 20 rows

   * 
   * 
   */
  
}