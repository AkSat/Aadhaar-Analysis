package com.aadhaar.dataframe
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.Row
import org.apache.log4j.Logger
import org.apache.log4j.Level


/*
 * 1. View/result of the top 25 rows from each individual store
 * 
 */


object kpi1ques1 extends App{
  
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
                
  
  //val df = session.read.schema(schema1).csv("F:/DataFlair - Spark and Scala/LMS Downloads/Projects/aadhaar_data.csv").toDF()
  
  //val df = session.read.format("csv").schema(schema1).load("F:/DataFlair - Spark and Scala/LMS Downloads/Projects/aadhaar_data.csv").toDF()
  
  val csv1 = session.sparkContext.textFile("F:/DataFlair - Spark and Scala/LMS Downloads/Projects/aadhaar_data.csv")
  val df1 = csv1.map(f => {
    val arr = f.split(",")
    Row(arr(0),arr(1),arr(2),arr(3),arr(4),arr(5),arr(6),arr(7),arr(8).toInt,arr(9).toInt,arr(10).toInt,arr(11).toInt,arr(12).toInt)
  })
  
  val df = session.createDataFrame(df1, schema1)
  
//  println("------ Type: -----")
//  println(df.getClass())
  println("=>  Using dataframe operations -------------")
  
  println("---------------- top 25 rows -------------")
  df.take(25).foreach(println)
  
  df.createOrReplaceTempView("aadhaar")
 
  println("\n\n=>  Using spark sql -------------")
  
  println("---------------- top 25 rows -------------")
  session.sql("select * from aadhaar limit 25").show(true)  
  
  /*
   * Output:
   * =>  Using dataframe operations -------------
    ---------------- top 25 rows -------------
    [20150420,Allahabad Bank,A-Onerealtors Pvt Ltd,Delhi,South Delhi,Defence Colony,110025,F,49,1,0,0,1]
    [20150420,Allahabad Bank,A-Onerealtors Pvt Ltd,Delhi,South Delhi,Defence Colony,110025,F,65,1,0,0,0]
    [20150420,Allahabad Bank,A-Onerealtors Pvt Ltd,Delhi,South Delhi,Defence Colony,110025,M,42,1,0,0,1]
    [20150420,Allahabad Bank,A-Onerealtors Pvt Ltd,Delhi,South Delhi,Defence Colony,110025,M,61,1,0,0,1]
    [20150420,Allahabad Bank,A-Onerealtors Pvt Ltd,Uttar Pradesh,Ambedkar Nagar,Akbarpur,224122,F,45,1,0,0,1]
    [20150420,Allahabad Bank,A-Onerealtors Pvt Ltd,Uttar Pradesh,Ambedkar Nagar,Akbarpur,224122,F,55,1,0,0,1]
    [20150420,Allahabad Bank,A-Onerealtors Pvt Ltd,Uttar Pradesh,Ambedkar Nagar,Akbarpur,224122,F,60,1,0,0,0]
    [20150420,Allahabad Bank,A-Onerealtors Pvt Ltd,Uttar Pradesh,Ambedkar Nagar,Akbarpur,224122,F,65,1,0,0,1]
    [20150420,Allahabad Bank,A-Onerealtors Pvt Ltd,Uttar Pradesh,Ambedkar Nagar,Akbarpur,224151,F,14,1,0,0,1]
    [20150420,Allahabad Bank,A-Onerealtors Pvt Ltd,Uttar Pradesh,Ambedkar Nagar,Akbarpur,224151,M,47,1,0,0,1]
    [20150420,Allahabad Bank,A-Onerealtors Pvt Ltd,Uttar Pradesh,Ambedkar Nagar,Akbarpur,224152,F,14,1,0,0,1]
    [20150420,Allahabad Bank,A-Onerealtors Pvt Ltd,Uttar Pradesh,Ambedkar Nagar,Akbarpur,224152,F,20,1,0,0,1]
    [20150420,Allahabad Bank,A-Onerealtors Pvt Ltd,Uttar Pradesh,Ambedkar Nagar,Akbarpur,224152,F,28,1,0,0,0]
    [20150420,Allahabad Bank,A-Onerealtors Pvt Ltd,Uttar Pradesh,Ambedkar Nagar,Akbarpur,224152,F,38,1,0,0,0]
    [20150420,Allahabad Bank,A-Onerealtors Pvt Ltd,Uttar Pradesh,Ambedkar Nagar,Akbarpur,224152,F,50,1,0,0,0]
    [20150420,Allahabad Bank,A-Onerealtors Pvt Ltd,Uttar Pradesh,Ambedkar Nagar,Akbarpur,224152,F,54,1,0,0,0]
    [20150420,Allahabad Bank,A-Onerealtors Pvt Ltd,Uttar Pradesh,Ambedkar Nagar,Akbarpur,224152,F,72,1,0,0,0]
    [20150420,Allahabad Bank,A-Onerealtors Pvt Ltd,Uttar Pradesh,Ambedkar Nagar,Akbarpur,224155,F,6,1,0,0,1]
    [20150420,Allahabad Bank,A-Onerealtors Pvt Ltd,Uttar Pradesh,Ambedkar Nagar,Akbarpur,224155,F,7,2,0,0,2]
    [20150420,Allahabad Bank,A-Onerealtors Pvt Ltd,Uttar Pradesh,Ambedkar Nagar,Akbarpur,224155,F,8,2,0,0,2]
    [20150420,Allahabad Bank,A-Onerealtors Pvt Ltd,Uttar Pradesh,Ambedkar Nagar,Akbarpur,224155,F,9,3,0,0,3]
    [20150420,Allahabad Bank,A-Onerealtors Pvt Ltd,Uttar Pradesh,Ambedkar Nagar,Akbarpur,224155,F,10,1,0,0,1]
    [20150420,Allahabad Bank,A-Onerealtors Pvt Ltd,Uttar Pradesh,Ambedkar Nagar,Akbarpur,224155,F,12,2,0,0,2]
    [20150420,Allahabad Bank,A-Onerealtors Pvt Ltd,Uttar Pradesh,Ambedkar Nagar,Akbarpur,224155,F,13,3,0,0,3]
    [20150420,Allahabad Bank,A-Onerealtors Pvt Ltd,Uttar Pradesh,Ambedkar Nagar,Akbarpur,224155,F,14,1,0,0,1]
    
    
    =>  Using spark sql -------------
    ---------------- top 25 rows -------------
    +--------+--------------+--------------------+-------------+--------------+--------------+-------+------+---+---------+--------+--------+-------+
    |    date|     registrar|       privateAgency|        state|      district|   subdistrict|pincode|gender|age|generated|rejected|mobileno|emailid|
    +--------+--------------+--------------------+-------------+--------------+--------------+-------+------+---+---------+--------+--------+-------+
    |20150420|Allahabad Bank|A-Onerealtors Pvt...|        Delhi|   South Delhi|Defence Colony| 110025|     F| 49|        1|       0|       0|      1|
    |20150420|Allahabad Bank|A-Onerealtors Pvt...|        Delhi|   South Delhi|Defence Colony| 110025|     F| 65|        1|       0|       0|      0|
    |20150420|Allahabad Bank|A-Onerealtors Pvt...|        Delhi|   South Delhi|Defence Colony| 110025|     M| 42|        1|       0|       0|      1|
    |20150420|Allahabad Bank|A-Onerealtors Pvt...|        Delhi|   South Delhi|Defence Colony| 110025|     M| 61|        1|       0|       0|      1|
    |20150420|Allahabad Bank|A-Onerealtors Pvt...|Uttar Pradesh|Ambedkar Nagar|      Akbarpur| 224122|     F| 45|        1|       0|       0|      1|
    |20150420|Allahabad Bank|A-Onerealtors Pvt...|Uttar Pradesh|Ambedkar Nagar|      Akbarpur| 224122|     F| 55|        1|       0|       0|      1|
    |20150420|Allahabad Bank|A-Onerealtors Pvt...|Uttar Pradesh|Ambedkar Nagar|      Akbarpur| 224122|     F| 60|        1|       0|       0|      0|
    |20150420|Allahabad Bank|A-Onerealtors Pvt...|Uttar Pradesh|Ambedkar Nagar|      Akbarpur| 224122|     F| 65|        1|       0|       0|      1|
    |20150420|Allahabad Bank|A-Onerealtors Pvt...|Uttar Pradesh|Ambedkar Nagar|      Akbarpur| 224151|     F| 14|        1|       0|       0|      1|
    |20150420|Allahabad Bank|A-Onerealtors Pvt...|Uttar Pradesh|Ambedkar Nagar|      Akbarpur| 224151|     M| 47|        1|       0|       0|      1|
    |20150420|Allahabad Bank|A-Onerealtors Pvt...|Uttar Pradesh|Ambedkar Nagar|      Akbarpur| 224152|     F| 14|        1|       0|       0|      1|
    |20150420|Allahabad Bank|A-Onerealtors Pvt...|Uttar Pradesh|Ambedkar Nagar|      Akbarpur| 224152|     F| 20|        1|       0|       0|      1|
    |20150420|Allahabad Bank|A-Onerealtors Pvt...|Uttar Pradesh|Ambedkar Nagar|      Akbarpur| 224152|     F| 28|        1|       0|       0|      0|
    |20150420|Allahabad Bank|A-Onerealtors Pvt...|Uttar Pradesh|Ambedkar Nagar|      Akbarpur| 224152|     F| 38|        1|       0|       0|      0|
    |20150420|Allahabad Bank|A-Onerealtors Pvt...|Uttar Pradesh|Ambedkar Nagar|      Akbarpur| 224152|     F| 50|        1|       0|       0|      0|
    |20150420|Allahabad Bank|A-Onerealtors Pvt...|Uttar Pradesh|Ambedkar Nagar|      Akbarpur| 224152|     F| 54|        1|       0|       0|      0|
    |20150420|Allahabad Bank|A-Onerealtors Pvt...|Uttar Pradesh|Ambedkar Nagar|      Akbarpur| 224152|     F| 72|        1|       0|       0|      0|
    |20150420|Allahabad Bank|A-Onerealtors Pvt...|Uttar Pradesh|Ambedkar Nagar|      Akbarpur| 224155|     F|  6|        1|       0|       0|      1|
    |20150420|Allahabad Bank|A-Onerealtors Pvt...|Uttar Pradesh|Ambedkar Nagar|      Akbarpur| 224155|     F|  7|        2|       0|       0|      2|
    |20150420|Allahabad Bank|A-Onerealtors Pvt...|Uttar Pradesh|Ambedkar Nagar|      Akbarpur| 224155|     F|  8|        2|       0|       0|      2|
    +--------+--------------+--------------------+-------------+--------------+--------------+-------+------+---+---------+--------+--------+-------+
    only showing top 20 rows

   * 
   * 
   */
}