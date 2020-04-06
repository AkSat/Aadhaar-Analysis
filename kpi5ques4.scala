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
 * 1. The top 3 states where the percentage of Aadhaar cards being generated for females is the
	highest.
	2. In each of these 3 states, identify the top 3 districts where the percentage of Aadhaar cards
	being rejected for males is the highest.
 * 
 */


object kpi5ques4 extends App{
  
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
  
  print("---------------- top 3 states where the percentage of Aadhaar cards being generated for females is the highest -------------\n")  
  val st = df.where("gender = 'F'").groupBy("state").agg(bround(sum("generated")/totalApplicants * 100,7).as("PercGenerated"))
    .orderBy(desc("PercGenerated")).select("state").rdd.map(f => f(0)).collect().toList
  
  st.take(3).foreach(println)
  
  println("\n-- In the above three states, below are top 3 districts where the percentage of Aadhaar cards being rejected for females is the highest -------------\n")
  
  val listState = st.take(3).toList
  
  for(i <- listState){
     df.filter(col("gender") === "M" && col("state") === i).groupBy("district").agg(bround(sum("rejected")/totalApplicants * 100,7).as("PercRejected"))
    .orderBy(desc("PercRejected")).take(3).map(f => (f(0),f(1))).foreach(f => println(i + ", " + f._1 + ", " + f._2))
  }
  

   
  println("\n=>  Using spark sql -------------")
  
  df.createOrReplaceTempView("aadhaar")
  //val listS = st.take(3).map(f => "'" + f + "'").mkString(",")
  
  for(i <- listState){
    val s = "'" + i + "'"
    println("State : " + i )
    session.sql(s"select district,(sum(rejected)/$totalApplicants * 100) as PercRejected from aadhaar where gender = 'M' and state = $s group by district order by PercRejected desc limit 3").show()
  }
  
  /*
   * 
   * Output :
   * 
   
   => Using dataframe operations -------------
    ---------------- top 3 states where the percentage of Aadhaar cards being generated for females is the highest -------------
    Maharashtra
    Uttar Pradesh
    Andhra Pradesh
    
    -- In the above three states, below are top 3 districts where the percentage of Aadhaar cards being rejected for females is the highest -------------
    
    Maharashtra, Nagpur, 0.0661921
    Maharashtra, Thane, 0.0649752
    Maharashtra, Mumbai, 0.043452
    Uttar Pradesh, Ghaziabad, 0.0194191
    Uttar Pradesh, Lucknow, 0.0162248
    Uttar Pradesh, Agra, 0.015084
    Andhra Pradesh, Kurnool, 0.1065766
    Andhra Pradesh, Srikakulam, 0.0421591
    Andhra Pradesh, Mahabub Nagar, 0.0417281
    
    =>  Using spark sql -------------
    State : Maharashtra
    +--------+--------------------+
    |district|        PercRejected|
    +--------+--------------------+
    |  Nagpur| 0.06619209142671717|
    |   Thane| 0.06497523183710306|
    |  Mumbai|0.043452027845803605|
    +--------+--------------------+
    
    State : Uttar Pradesh
    +---------+--------------------+
    | district|        PercRejected|
    +---------+--------------------+
    |Ghaziabad|0.019419050950925068|
    |  Lucknow|0.016224794528188046|
    |     Agra|0.015083988662924824|
    +---------+--------------------+
    
    State : Andhra Pradesh
    +-------------+--------------------+
    |     district|        PercRejected|
    +-------------+--------------------+
    |      Kurnool| 0.10657661905703522|
    |   Srikakulam|0.042159114531838626|
    |Mahabub Nagar| 0.04172814342718363|
    +-------------+--------------------+

   
   * 
   */
  
  
}