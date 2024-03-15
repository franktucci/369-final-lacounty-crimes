import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ModusOperandiAnalysis {
  def main(args: Array[String]): Unit = {
    // Initialize SparkSession
    val spark = SparkSession.builder()
      .appName("Modus Operandi Analysis")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    // Read the CSV file
    val crimesDF = spark.read
      .option("header", "true")
      .csv("src/main/Crime_Data_from_2020_to_Present.csv")

    // Split the Mocodes column into individual modus operandi
    val crimesWithModusOperandi = crimesDF
      .withColumn("Modus_Operandi", split($"Mocodes", " "))

    // Explode the Modus_Operandi array into separate rows
    val explodedCrimes = crimesWithModusOperandi
      .select($"DR_NO", explode($"Modus_Operandi").as("Modus_Operandi"))

    // Group crimes by their modus operandi and count occurrences
    val modusOperandiCounts = explodedCrimes
      .groupBy("Modus_Operandi")
      .agg(count("*").alias("Occurrences"))
      .sort($"Occurrences".desc)

    // Display the result
    modusOperandiCounts.show(false)

    // Stop SparkSession
    spark.stop()
  }
}
