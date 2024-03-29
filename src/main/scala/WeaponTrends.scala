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
object WeaponTrends extends JFXApp{

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  val conf = new SparkConf().setAppName("App")
    .setMaster("local[4]")
  val sc = new SparkContext(conf)

  val weaponsRDD = sc.textFile("Big_Crime_Data_from_2020_to_Present.csv").map { data =>
    val parts = data.split(",")
    val areaCode = parts(4)
    val weaponType = parts(16)
    val weaponCode = parts(17)
    (weaponCode, 1)
  }.reduceByKey(_ + _)
    .sortBy(_._2, ascending = false) // sort by descending order
    .take(11)

  val weaponsTypeRDD = sc.textFile("Crime_Data_from_2020_to_Present.csv").map { data =>
    val parts = data.split(",")
    val areaCode = parts(4)
    val weaponType = parts(16)
    val weaponCode = parts(17)
    (weaponType, 1)
  }.reduceByKey(_ + _)
    .sortBy(_._2, ascending = false) // sort by descending order
    .take(11)

  weaponsRDD.foreach { case (codeName, count) =>
    println(s"$codeName, $count")
  }
  println()
  weaponsTypeRDD.foreach { case (codeName, count) =>
    println(s"$codeName, $count")
  }

  sc.stop()

  val areaDataForChart = ObservableBuffer(weaponsRDD.map { case (codeName, count) =>
    XYChart.Data[String, Number](codeName, count)
  }: _*)

  stage = new PrimaryStage {
    title = "Crime Data Analysis"
    scene = new Scene {
      val xAxis = new CategoryAxis {
        label = "Description"
      }
      val yAxis = new NumberAxis {
        label = "Count"
      }
      root = new BarChart[String, Number](xAxis, yAxis) {
        title = "Common Weapons Involved in Crimes"
        data = Seq(XYChart.Series("Counts", areaDataForChart))
      }
    }
  }

}
