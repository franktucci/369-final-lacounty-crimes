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
// realjust
object DemographicTrends extends JFXApp {
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  val conf = new SparkConf().setAppName("App")
    .setMaster("local[1]")
  val sc = new SparkContext(conf)

  val demographicRDD = sc.textFile("Crime_Data_from_2020_to_Present.csv").map { data =>
    val parts = data.split(",")
    val decent = parts(13)
    (decent, 1)
  }.reduceByKey(_ + _)
    .sortBy(_._2, ascending = false) // sort by descending order
    .take(5)

  demographicRDD.foreach { case (decent, count) =>
    println(s"$decent, $count")
  }
  sc.stop()

  val dataForChart = ObservableBuffer(demographicRDD.map { case (descent, count) =>
    XYChart.Data[String, Number](descent, count)
  }: _*)

  stage = new PrimaryStage {
    title = "Crime Data Analysis"
    scene = new Scene {
      val xAxis = new CategoryAxis {
        label = "Descent"
      }
      val yAxis = new NumberAxis {
        label = "Count"
      }
      root = new BarChart[String, Number](xAxis, yAxis) {
        title = "Top 5 Descents by Crime Count"
        data = Seq(XYChart.Series("Counts", dataForChart))
      }
    }
  }
}
