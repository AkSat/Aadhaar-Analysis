package com.aadhaar.dataframe
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.col
import org.apache.log4j.Logger
import org.apache.log4j.Level


/*
 * 1. Find the count and names of registrars in the table.
 * 
 */


object kpi2ques1 extends App{
  
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
  
  println("=>  Using dataframe operations -------------")
  
  val df = session.createDataFrame(df1, schema1)
  
  println("---------------- Count and names of registrars -------------")
  df.groupBy(col("registrar")).count().show()
  
  
  println("\n\n=>  Using spark sql -------------")
  
  println("---------------- Count and names of registrars -------------")
  df.createOrReplaceTempView("aadhaar")
  session.sql("select registrar,count(*) as cnt from aadhaar group by registrar").show(true)  
  
/*
 * Output:
 * =>  Using dataframe operations -------------
    ---------------- Count and names of registrars -------------
    +--------------------+------+
    |           registrar| count|
    +--------------------+------+
    |Govt of Andhra Pr...| 47489|
    | UT Of Daman and Diu|  1786|
    |Govt of Madhya Pr...|  3582|
    |Punjab National Bank|  3303|
    |NSDL e-Governance...|122709|
    |       IDBI Bank ltd|  9218|
    |    Delhi - North DC|  4372|
    | Govt of Maharashtra|165780|
    |           Indiapost| 22116|
    |  FCS Govt of Punjab| 29937|
    |Govt of Sikkim - ...|   783|
    |       Delhi - NE DC|  1516|
    | FCR Govt of Haryana|  7959|
    |               IGNOU|   124|
    |Principal Revenue...|  2157|
    |      Syndicate Bank|    25|
    |Indian Overseas Bank|    52|
    |     Delhi - East DC|  1957|
    |Department of Inf...|  3772|
    | Bank of Maharashtra|  2723|
    +--------------------+------+
    only showing top 20 rows
    
    
    
    =>  Using spark sql -------------
    ---------------- Count and names of registrars -------------
    +--------------------+------+
    |           registrar|   cnt|
    +--------------------+------+
    |Govt of Andhra Pr...| 47489|
    | UT Of Daman and Diu|  1786|
    |Govt of Madhya Pr...|  3582|
    |Punjab National Bank|  3303|
    |NSDL e-Governance...|122709|
    |       IDBI Bank ltd|  9218|
    |    Delhi - North DC|  4372|
    | Govt of Maharashtra|165780|
    |           Indiapost| 22116|
    |  FCS Govt of Punjab| 29937|
    |Govt of Sikkim - ...|   783|
    |       Delhi - NE DC|  1516|
    | FCR Govt of Haryana|  7959|
    |               IGNOU|   124|
    |Principal Revenue...|  2157|
    |      Syndicate Bank|    25|
    |Indian Overseas Bank|    52|
    |     Delhi - East DC|  1957|
    |Department of Inf...|  3772|
    | Bank of Maharashtra|  2723|
    +--------------------+------+
    only showing top 20 rows

 * 
 * 
 */
  
}