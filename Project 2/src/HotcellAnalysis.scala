package cse512

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._

object HotcellAnalysis {
  Logger.getLogger("org.spark_project").setLevel(Level.WARN)
  Logger.getLogger("org.apache").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)

def runHotcellAnalysis(spark: SparkSession, pointPath: String): DataFrame =
{
  // Load the original data from a data source
  var pickupInfo = spark.read.format("com.databricks.spark.csv").option("delimiter",";").option("header","false").load(pointPath);
  pickupInfo.createOrReplaceTempView("nyctaxitrips")
  pickupInfo.show()

  // Assign cell coordinates based on pickup points
  spark.udf.register("CalculateX",(pickupPoint: String)=>((
    HotcellUtils.CalculateCoordinate(pickupPoint, 0)
    )))
  spark.udf.register("CalculateY",(pickupPoint: String)=>((
    HotcellUtils.CalculateCoordinate(pickupPoint, 1)
    )))
  spark.udf.register("CalculateZ",(pickupTime: String)=>((
    HotcellUtils.CalculateCoordinate(pickupTime, 2)
    )))
  pickupInfo = spark.sql("select CalculateX(nyctaxitrips._c5),CalculateY(nyctaxitrips._c5), CalculateZ(nyctaxitrips._c1) from nyctaxitrips")
  var newCoordinateName = Seq("x", "y", "z")
  pickupInfo = pickupInfo.toDF(newCoordinateName:_*)
  pickupInfo.show()

  // Define the min and max of x, y, z
  val minX = -74.50/HotcellUtils.coordinateStep
  val maxX = -73.70/HotcellUtils.coordinateStep
  val minY = 40.50/HotcellUtils.coordinateStep
  val maxY = 40.90/HotcellUtils.coordinateStep
  val minZ = 1
  val maxZ = 31
  val numCells = (maxX - minX + 1)*(maxY - minY + 1)*(maxZ - minZ + 1)

  // YOU NEED TO CHANGE THIS PART

  pickupInfo.createOrReplaceTempView("pickupInfo")
  val cellsTable = spark.sql(s"""
  SELECT x, y, z, COUNT(*) AS x_j
  FROM pickupInfo
  WHERE x >= $minX AND x <= $maxX AND y >= $minY AND y <= $maxY AND z >= $minZ AND z <= $maxZ
  GROUP BY z, y, x
  ORDER BY z, y, x
  """)
  cellsTable.createOrReplaceTempView("cellsTable")

  val cells = spark.sql("SELECT x, y, z, x_j FROM cellsTable")
  cells.createOrReplaceTempView("cells")

  // Compute the values and summations for calculating the G score
  val x_j_query = spark.sql(""" SELECT SUM(x_j) AS sum_x_j, SUM(x_j * x_j) AS sum_x_j_squared
  FROM cells
  """)
  val sum_x_j = x_j_query.first().getAs[Long]("sum_x_j")
  val X = sum_x_j.toDouble/numCells.toDouble
  val sum_x_j_squared = x_j_query.first().getAs[Long]("sum_x_j_squared")
  val S = Math.sqrt((sum_x_j_squared.toDouble/numCells) - (X * X))

    spark.udf.register("countNumAdjacentCells", (cellX: Int, cellY: Int, cellZ: Int, minX: Int, maxX: Int, minY: Int, maxY: Int, minZ: Int, maxZ: Int) => (HotcellUtils.countNumAdjacentCells(cellX, cellY, cellZ, minX, 
  maxX, minY, maxY, minZ, maxZ)))


  // Calculate the number of adjacent cells for each cell
  val adjacentCells = spark.sql("""
  SELECT c1.x AS cellX, c1.y AS cellY, c1.z AS cellZ,
  COUNT(*) AS numNeighbors, countNumAdjacentCells(c1.x, c1.y, c1.z, """ + minX + """, """ + maxX + """, """ +  minY + 
  """, """ + maxY + """, """ + minZ + """, """ + maxZ + """) as adjacentCellCount, SUM(c2.x_j) as x_j_times_w_ij
  FROM cells c1
  JOIN cells c2 ON (c1.x - 1 <= c2.x AND c1.x + 1 >= c2.x AND
                    c1.y - 1 <= c2.y AND c1.y + 1 >= c2.y AND
                    c1.z - 1 <= c2.z AND c1.z + 1 >= c2.z)
  GROUP BY c1.z, c1.y, c1.x
  ORDER BY c1.z, c1.y, c1.x
  """)

  spark.udf.register("computeGScore", (sum_w_ij_x_j: Double, mean: Double, weight: Double, S: Double, numCells: Int) => (HotcellUtils.computeGScore(sum_w_ij_x_j, mean, weight, 
  S, numCells)))

  val Gscores = spark.sql(s"""
  SELECT cellX, cellY, cellZ, computeGScore(${adjacentCells("x_j_times_w_ij")}, $X, ${adjacentCells("adjacentCellCount")}, $S, $numCells) AS Gscore
  FROM adjacentCells
  ORDER BY Gscore desc
""")




  Gscores.createOrReplaceTempView("Gscores")
  val resultDf = spark.sql("select cellX, cellY, cellZ from Gscores")
  resultDf.createOrReplaceTempView("resultDf")

  return resultDf // YOU NEED TO CHANGE THIS PART
}
}
