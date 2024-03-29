import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}

object Dates {

  // DR_NO, -> 0
  // Date Rptd, -> 1
  // DATE OCC, -> 2
  // TIME OCC, -> 3
  // AREA, -> 4
  // AREA NAME, -> 5
  // Rpt Dist No, -> 6
  // Part 1-2, -> 7
  // Crm Cd, -> 8
  // Crm Cd Desc, -> 9
  // Mocodes, -> 10
  // Vict Age, -> 11
  // Vict Sex, -> 12
  // Vict Descent, -> 13
  // Premis Cd, -> 14
  // Premis Desc, -> 15
  // Weapon Used Cd, -> 16
  // Weapon Desc, -> 17
  // Status, -> 18
  // Status Desc, -> 19
  // Crm Cd 1, -> 20
  // Crm Cd 2, -> 21
  // Crm Cd 3, -> 22
  // Crm Cd 4, -> 23
  // LOCATION, -> 24
  // Cross Street, -> 25
  // LAT, -> 26
  // LON -> 27

  var crimeDataPath = "/Users/wintongee/Desktop/School/Winter 2024/CSC369/Repos/Final Project/src/main/scala/CrimeData.csv"

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val conf = new SparkConf().setAppName("369FINALPROJECT").setMaster("local[4]")
    val sc = new SparkContext(conf)

    val spark = SparkSession.builder().getOrCreate()
    val crimeDataRDD = spark.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(crimeDataPath)
      .rdd

    val totalCrimes = crimeDataRDD.count()

    val females = crimeDataRDD
      .map(row => row(12))
      .filter(victimSex => victimSex != null && victimSex.equals("F"))
      .count()

    val males = crimeDataRDD
      .map(row => row(12))
      .filter(victimSex => victimSex != null && victimSex.equals("M"))
      .count()

    val averageAge = crimeDataRDD
      .map(row => (row(11).toString))
      .filter(victimAge => victimAge != null)
      .map(item => item.toInt)
      .collect()
      .sum / (females + males) // No Built in average

    val averageAgeMale = crimeDataRDD
      .map(row => (row(11).toString, row(12)))
      .filter { // Make sure inputs are valid and is male
        case (age, gender) =>
          age != null && gender != null && gender.equals("M")
      }
      .map { // Remove gender and convert to int
        case (age, gender) =>
          age.toInt
      }
      .collect()
      .sum / males // No Built in average func

    val averageAgeFemale = crimeDataRDD
      .map(row => (row(11).toString, row(12)))
      .filter { // Make sure inputs are valid and is female
        case (age, gender) =>
          age != null && gender != null && gender.equals("F")
      }
      .map { // Remove gender and convert to int
        case (age, gender) =>
          age.toInt
      }
      .collect()
      .sum / females // No Built in average func

    val crimesWithVictim = 100 * (females + males) / totalCrimes
    val crimesWithoutVictim = 100 - crimesWithVictim


    val mostDangerousDates = crimeDataRDD
      .map {
        row =>
          val date = row(1).toString.split(" ")(0)
          val weapon = row(16)
          (date, weapon)
      }
      .groupByKey() // Group by the date
      .mapValues(_.toList.size) // Map for number of weapons on day
      .sortBy(-1 * _._2) // Sort by weapon count in descending order
      .take(10) // Top 10

    System.out.println("Total Crimes: " + totalCrimes + ", Females: " + females + ", Males: " + males)
    System.out.println("Crimes that had victims: " + crimesWithVictim + "%" + ", Without: " + crimesWithoutVictim + "%")
    System.out.println("Average victim age of Females: " + averageAgeFemale + ", Males: " + averageAgeMale)

    System.out.println("\nMost Dangerous Dates")
    mostDangerousDates.foreach(println)
  }
}
