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
 * 2. Find the number of states, districts in each state and sub-districts in each district.
 * 
 */


object kpi2ques2 extends App{
  
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
  
  // ---- the number of states
  println("================> Number of states : " + df.select(col("state")).distinct().count())
  
  println("-------------------- Number of districts in each state -------------------- ")
  // ---- the number of districts in each state
  df.select("state","district").groupBy("state").agg(countDistinct("district").alias("No. of Districts")).sort("state").show()
  
  println("-------------------- Number of subdistricts in each district -------------------- ")
  // ---- the number of sub-districts in each district.
  df.select("district","subdistrict").groupBy("district").agg(countDistinct("subdistrict").alias("No. of Subdistricts")).sort("district").show()
  
  
  println("\n\n=>  Using spark sql -------------")
  df.createOrReplaceTempView("aadhaar")
  println("-------------------- Number of states -------------------- : ")
  session.sql("select count(*) as NoOfStates from (select distinct state from aadhaar)").show(true)
  
  println("-------------------- Number of districts in each state -------------------- ")
  session.sql("select state,count(distinct(district)) as cnt from aadhaar group by state order by state").show(true)
  
  println("-------------------- Number of subdistricts in each district -------------------- ")
  session.sql("select district,count(distinct(subdistrict)) as cnt from aadhaar group by district order by district").show(true)  
  
  /*
   * Output :=>  
   * Using dataframe operations -------------
   * ================> Number of states : 37
   * -------------------- Number of districts in each state -------------------- 
   * +--------------------+----------------+
   * |               state|No. of Districts|
   * +--------------------+----------------+
   * |Andaman and Nicob...|               4|
   * |      Andhra Pradesh|              29|
   * |   Arunachal Pradesh|              16|
   * |               Assam|              27|
   * |               Bihar|              40|
   * |          Chandigarh|               2|
   * |        Chhattisgarh|              34|
   * |Dadra and Nagar H...|               1|
   * |       Daman and Diu|               2|
   * |               Delhi|              11|
   * |                 Goa|               7|
   * |             Gujarat|              33|
   * |             Haryana|              21|
   * |    Himachal Pradesh|              13|
   * |   Jammu and Kashmir|              24|
   * |           Jharkhand|              29|
   * |           Karnataka|              44|
   * |              Kerala|              15|
   * |         Lakshadweep|               1|
   * |      Madhya Pradesh|              50|
   * +--------------------+----------------+
   * only showing top 20 rows
   * 
   * -------------------- Number of subdistricts in each district -------------------- 
   * +--------------+-------------------+
   * |      district|No. of Subdistricts|
   * +--------------+-------------------+
   * |      Adilabad|                 54|
   * |          Agra|                  8|
   * |     Ahmadabad|                  7|
   * |    Ahmadnagar|                 14|
   * |   Ahmed Nagar|                 16|
   * |     Ahmedabad|                 11|
   * |    Ahmednagar|                 14|
   * |        Aizawl|                  6|
   * |         Ajmer|                 10|
   * |         Akola|                  8|
   * |     Alappuzha|                 11|
   * |       Aligarh|                  7|
   * |     Alirajpur|                  2|
   * |     Allahabad|                 12|
   * |        Almora|                  5|
   * |         Alwar|                 13|
   * |        Ambala|                  4|
   * |Ambedkar Nagar|                  6|
   * |        Amethi|                  5|
   * |      Amravati|                 16|
   * +--------------+-------------------+
   * only showing top 20 rows
   * 
   * =>  Using spark sql -------------
   * -------------------- Number of states -------------------- : 
   * +----------+
   * |NoOfStates|
   * +----------+
   * |        37|
   * +----------+
   * 
   * -------------------- Number of districts in each state -------------------- 
   * +--------------------+---+
   * |               state|cnt|
   * +--------------------+---+
   * |Andaman and Nicob...|  4|
   * |      Andhra Pradesh| 29|
   * |   Arunachal Pradesh| 16|
   * |               Assam| 27|
   * |               Bihar| 40|
   * |          Chandigarh|  2|
   * |        Chhattisgarh| 34|
   * |Dadra and Nagar H...|  1|
   * |       Daman and Diu|  2|
   * |               Delhi| 11|
   * |                 Goa|  7|
   * |             Gujarat| 33|
   * |             Haryana| 21|
   * |    Himachal Pradesh| 13|
   * |   Jammu and Kashmir| 24|
   * |           Jharkhand| 29|
   * |           Karnataka| 44|
   * |              Kerala| 15|
   * |         Lakshadweep|  1|
   * |      Madhya Pradesh| 50|
   * +--------------------+---+
   * only showing top 20 rows
   * 
   * -------------------- Number of subdistricts in each district -------------------- 
   * +--------------+---+
   * |      district|cnt|
   * +--------------+---+
   * |      Adilabad| 54|
   * |          Agra|  8|
   * |     Ahmadabad|  7|
   * |    Ahmadnagar| 14|
   * |   Ahmed Nagar| 16|
   * |     Ahmedabad| 11|
   * |    Ahmednagar| 14|
   * |        Aizawl|  6|
   * |         Ajmer| 10|
   * |         Akola|  8|
   * |     Alappuzha| 11|
   * |       Aligarh|  7|
   * |     Alirajpur|  2|
   * |     Allahabad| 12|
   * |        Almora|  5|
   * |         Alwar| 13|
   * |        Ambala|  4|
   * |Ambedkar Nagar|  6|
   * |        Amethi|  5|
   * |      Amravati| 16|
   * +--------------+---+
   * only showing top 20 rows
   * 
   *   
  */
  
}