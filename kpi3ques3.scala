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
 * 3. Find the number of residents providing email, mobile number? (Hint: consider non-zero values.)
 * 
 */


object kpi3ques3 extends App{
  
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
  print("-- Number of residents providing email, mobile number using filter clause : ")
  
  print(df.select("emailid","mobileno").filter(col("emailid") =!= 0  && col("mobileno") =!= 0).count())
  
  
  // ------- Important employees.filter($"emp_id".isin(items:_*)).show
  print("\n-- Number of residents providing email, mobile number using where clause : ")
  print(df.select("emailid","mobileno").where("emailid != 0 AND mobileno != 0").count())
  
   
  println("\n=>  Using spark sql -------------")
  println("-- Number of residents providing email, mobile number : ")
  df.createOrReplaceTempView("aadhaar")
  session.sql("select count(*) as total from aadhaar where emailid != 0 AND mobileno != 0").show() 
  
  /*
   * Output :
   * 
   * =>  Using dataframe operations -------------
	 * -- Number of residents providing email, mobile number using filter clause : 45624
	 * -- Number of residents providing email, mobile number using where clause : 45624
	 * =>  Using spark sql -------------
	 * -- Number of residents providing email, mobile number : 
	 * +-----+
	 * |total|
	 * +-----+
	 * |45624|
	 * +-----+
	 *
   * 
   */
  
}