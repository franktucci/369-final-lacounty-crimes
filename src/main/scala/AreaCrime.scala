import org.apache.spark.SparkContext._

import scala.io._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import scalafx.application.JFXApp
import scalafx.application.JFXApp.PrimaryStage
import scalafx.collections.ObservableBuffer
import scalafx.scene.Scene
import scalafx.scene.chart.{BarChart, CategoryAxis, NumberAxis, XYChart}
import scalafx.Includes._

import java.io._
import java.util.Observable
import scala.collection._

object AreaCrime extends JFXApp {
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  val conf = new SparkConf().setAppName("App")
    .setMaster("local[4]")
  val sc = new SparkContext(conf)

  val departmentAreaCodeRDD = sc.textFile("Crime_Data_from_2020_to_Present.csv").map { data =>
    val parts = data.split(",")
    val departmentCode = parts(4)
    val areaName = parts(5)
    (departmentCode, 1)
  }.reduceByKey(_ + _)
    .sortBy(_._2, ascending = true) // sort by descending order
    .take(11)

  departmentAreaCodeRDD.foreach { case (codeName, count) =>
    println(s"$codeName, $count")
  }
  sc.stop()

  val areaDataForChart = ObservableBuffer(departmentAreaCodeRDD.map { case (codeName, count) =>
    XYChart.Data[String, Number](codeName, count)}: _*)

  stage = new PrimaryStage {
    title = "Crime Data Analysis"
    scene = new Scene {
      val xAxis = new CategoryAxis {
        label = "Code"
      }
      val yAxis = new NumberAxis {
        label = "Count"
      }
      root = new BarChart[String, Number](xAxis, yAxis) {
        title = "Top 10 Safest Areas in LA County"
        data = Seq(XYChart.Series("Counts", areaDataForChart))
      }
    }
  }
}
