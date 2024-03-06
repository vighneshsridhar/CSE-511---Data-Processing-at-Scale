package cse512

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._

object HotzoneAnalysis {

  Logger.getLogger("org.spark_project").setLevel(Level.WARN)
  Logger.getLogger("org.apache").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)

  def runHotZoneAnalysis(spark: SparkSession, pointPath: String, rectanglePath: String): DataFrame = {

    var pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter",";").option("header","false").load(pointPath);
    pointDf.createOrReplaceTempView("point")

    // Parse point data formats
    spark.udf.register("trim",(string : String)=>(string.replace("(", "").replace(")", "")))
    pointDf = spark.sql("select trim(_c5) as _c5 from point")
    pointDf.createOrReplaceTempView("point")

    // Load rectangle data
    val rectangleDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(rectanglePath);
    rectangleDf.createOrReplaceTempView("rectangle")

    // Join two datasets
    spark.udf.register("ST_Contains",(queryRectangle:String, pointString:String)=>(HotzoneUtils.ST_Contains(queryRectangle, pointString)))
    val joinDf = spark.sql("select rectangle._c0 as rectangle, point._c5 as point from rectangle,point where ST_Contains(rectangle._c0,point._c5)")
    joinDf.createOrReplaceTempView("joinResult")

    // YOU NEED TO CHANGE THIS PART
    val joinDfCoord = joinDf.withColumn("x1", split(col("rectangle"), ",")(0).cast("double")).withColumn("y1", split(col("rectangle"), ",")(1).cast("double")).withColumn("x2", split(col("rectangle"), ",")(2).cast("double")).withColumn("y2", split(col("rectangle"), ",")(3).cast("double"))
    val countDf = joinDfCoord.groupBy("rectangle").agg(count("point").as("point_count"))
    val countDf_drop = countDf.drop(col("point"))
    val deduplicatedDf = joinDfCoord.dropDuplicates("rectangle")
    val resultDf = deduplicatedDf.join(countDf, Seq("rectangle"))
    val resultSortedDf = resultDf.sort(desc("x1"), desc("y1"), desc("x2"), desc("y2"))
    val resultFilteredDf = resultSortedDf.select("rectangle", "point_count").coalesce(1)

    return resultFilteredDf // YOU NEED TO CHANGE THIS PART
  }

}
