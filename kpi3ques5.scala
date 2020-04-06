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
 * 5. Find the no. of Aadhaar cards generated in each state?
 * 
 */


object kpi3ques5 extends App{
  
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
  println("---------------- Number of Aadhaar cards generated in each state -------------")
  
  df.select("state","generated").groupBy("state").agg(sum("generated").as("TotalGenerated")).sort("state").show()
  
   
  println("\n\n=>  Using spark sql -------------")
  println("---------------- Number of Aadhaar cards generated in each state -------------")
  
  df.createOrReplaceTempView("aadhaar")
  session.sql("select state,sum(generated) as total from aadhaar group by state order by state").show()
 
  /*
   * 
   * Output :
   * 
   * => Using dataframe operations -------------
    ---------------- Number of Aadhaar cards generated in each state -------------
    +--------------------+--------------+
    |               state|TotalGenerated|
    +--------------------+--------------+
    |Andaman and Nicob...|           636|
    |      Andhra Pradesh|        270055|
    |   Arunachal Pradesh|          1178|
    |               Assam|           891|
    |               Bihar|        208161|
    |          Chandigarh|          1978|
    |        Chhattisgarh|         59764|
    |Dadra and Nagar H...|           108|
    |       Daman and Diu|          2479|
    |               Delhi|         37156|
    |                 Goa|          7979|
    |             Gujarat|        189685|
    |             Haryana|         95350|
    |    Himachal Pradesh|         33844|
    |   Jammu and Kashmir|         17355|
    |           Jharkhand|        168855|
    |           Karnataka|        146013|
    |              Kerala|        150893|
    |         Lakshadweep|            15|
    |      Madhya Pradesh|        171324|
    +--------------------+--------------+
    only showing top 20 rows
    
    
    
    =>  Using spark sql -------------
    ---------------- Number of Aadhaar cards generated in each state -------------
    +--------------------+------+
    |               state| total|
    +--------------------+------+
    |Andaman and Nicob...|   636|
    |      Andhra Pradesh|270055|
    |   Arunachal Pradesh|  1178|
    |               Assam|   891|
    |               Bihar|208161|
    |          Chandigarh|  1978|
    |        Chhattisgarh| 59764|
    |Dadra and Nagar H...|   108|
    |       Daman and Diu|  2479|
    |               Delhi| 37156|
    |                 Goa|  7979|
    |             Gujarat|189685|
    |             Haryana| 95350|
    |    Himachal Pradesh| 33844|
    |   Jammu and Kashmir| 17355|
    |           Jharkhand|168855|
    |           Karnataka|146013|
    |              Kerala|150893|
    |         Lakshadweep|    15|
    |      Madhya Pradesh|171324|
    +--------------------+------+
    only showing top 20 rows

   * 
   * 
   * 
   */
  
}