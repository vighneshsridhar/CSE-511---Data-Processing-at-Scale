package cse512

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Calendar
import java.lang.Math

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Column

object HotcellUtils {
  val coordinateStep = 0.01

  def CalculateCoordinate(inputString: String, coordinateOffset: Int): Int =
  {
    // Configuration variable:
    // Coordinate step is the size of each cell on x and y
    var result = 0
    coordinateOffset match
    {
      case 0 => result = Math.floor((inputString.split(",")(0).replace("(","").toDouble/coordinateStep)).toInt
      case 1 => result = Math.floor(inputString.split(",")(1).replace(")","").toDouble/coordinateStep).toInt
      // We only consider the data from 2009 to 2012 inclusively, 4 years in total. Week 0 Day 0 is 2009-01-01
      case 2 => {
        val timestamp = HotcellUtils.timestampParser(inputString)
        result = HotcellUtils.dayOfMonth(timestamp) // Assume every month has 31 days
      }
    }
    return result
  }

  def timestampParser (timestampString: String): Timestamp =
  {
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
    val parsedDate = dateFormat.parse(timestampString)
    val timeStamp = new Timestamp(parsedDate.getTime)
    return timeStamp
  }

  def dayOfYear (timestamp: Timestamp): Int =
  {
    val calendar = Calendar.getInstance
    calendar.setTimeInMillis(timestamp.getTime)
    return calendar.get(Calendar.DAY_OF_YEAR)
  }

  def dayOfMonth (timestamp: Timestamp): Int =
  {
    val calendar = Calendar.getInstance
    calendar.setTimeInMillis(timestamp.getTime)
    return calendar.get(Calendar.DAY_OF_MONTH)
  }

  // YOU NEED TO CHANGE THIS PART


  def countNumAdjacentCells(cellX: Int, cellY: Int, cellZ: Int, minX: Int, maxX: Int, minY: Int, maxY: Int, minZ: Int, maxZ: Int): Int = 
  {
    var typeOfPoint = 0
    var numAdjacentCells = 0

    if (cellX == minX || cellX == maxX){
        typeOfPoint += 1;    
    }

    if (cellY == minY || cellY == maxY){
        typeOfPoint += 1;    
    }

    if (cellZ == minZ || cellZ == maxZ){
        typeOfPoint += 1;    
    }

    // Point is a center point
    if (typeOfPoint == 0){
        numAdjacentCells = 26
        return numAdjacentCells
    }

    // Point is a face point
    if (typeOfPoint == 1){
        numAdjacentCells = 17
        return numAdjacentCells
    }

    // Point is a edge point
    if (typeOfPoint == 2){
        numAdjacentCells = 11
        return numAdjacentCells
    }

    // Point is a corner point
    if (typeOfPoint == 3){
        numAdjacentCells = 7
        return numAdjacentCells
    }

    return numAdjacentCells
  }

  def computeGScore(sum_w_ij_x_j: Double, mean: Double, weight: Double, S: Double, numCells: Int): Double =
  {

    val G_score = ((sum_w_ij_x_j.toDouble - mean * weight.toDouble).toDouble / (S * Math.sqrt((numCells.toDouble * 
    weight.toDouble - weight.toDouble * weight.toDouble) / (numCells.toDouble - 1)).toDouble).toDouble).toDouble
    return G_score
  }

}
