import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import vegas._
import vegas.sparkExt._
import java.util.{Calendar, GregorianCalendar}

object Main
{
    def main(args: Array[String]): Unit =
    {
        Logger.getLogger("org").setLevel(Level.OFF)
        Logger.getLogger("akka").setLevel(Level.OFF)
        val conf = new SparkConf().setAppName("369FINALPROJECT").setMaster("local[4]")
        val sc = new SparkContext(conf)
        val spark = SparkSession.builder().getOrCreate()

        val df = spark.read
            .format("csv")
            .option("header", "true")
            .option("inferSchema", "true")
            .load("data/Crime_Data_from_2020_to_Present.csv")
        df.show()

        seasonalVariation(df, sc)
    }
    def seasonalVariation(df: DataFrame, sc: SparkContext): Unit =
    {
        val data: RDD[Row] = df.rdd
        val rdd = data.map[(String, Int, Int, Int, Int, String, Int, String, String)](r =>
            {
                val dateTokens = r(2).toString.split(" ")(0).split("/")
                val hour = f"${r(3).asInstanceOf[Int]}%04d".substring(0, 2).toInt
                val gender = if (r(12) == null) "X" else r(12).toString
                val weapon = if (r(17) == null) "None" else r(17).toString
                (r(0).toString, dateTokens(2).toInt, dateTokens(0).toInt, dateTokens(1).toInt, hour, r(9).toString, r(11).toString.toInt, gender, weapon)
            })
        val cols = Array("id", "year", "month", "day", "hour", "desc", "age", "gender", "weapon")
        var seasonalDf = df.sqlContext.createDataFrame(rdd)
        for (i <- Range(0, cols.length))
            seasonalDf = seasonalDf.withColumnRenamed("_" + (i + 1).toString, cols(i))

        // What are the 10 most popular crimes?
        val crimesRdd = seasonalDf.select("desc").rdd
            .map(x => (x(0).toString, 1))
            .reduceByKey(_ + _)
            .map({case (key, count) => (count, key)})
            .sortByKey(ascending=false)
            .collect.take(10)
        val top10Crimes = crimesRdd.map({ case (_, desc) => desc})

        // What are the 10 most popular weapons?
        val weaponsRdd = seasonalDf.select("weapon").rdd
            .map(x => (x(0).toString, 1))
            .filter({case (weapon, _) => weapon != "None"})
            .reduceByKey(_ + _)
            .map({case (key, count) => (count, key)})
            .sortByKey(ascending=false)
            .collect.take(10)
        val top10Weapons = weaponsRdd.map({ case (_, weapon) => weapon})

        // Bar chart of the top 10 classifications of crimes, in order
        var tempDf = df.sqlContext.createDataFrame(crimesRdd).withColumnRenamed("_1", "Crimes Committed").withColumnRenamed("_2", "Type")
        Vegas("Top 10 Types of Crimes")
            .withDataFrame(tempDf)
            .encodeX("Type", Ordinal, sortOrder=vegas.spec.Spec.SortOrderEnums.None)
            .encodeY("Crimes Committed", Quantitative)
            .mark(Bar)
            .show

        // Bar chart of the top 10 weapons used, in order
        tempDf = df.sqlContext.createDataFrame(weaponsRdd).withColumnRenamed("_1", "Crimes Committed").withColumnRenamed("_2", "Weapon")
        Vegas("Top 10 Weapons Used")
            .withDataFrame(tempDf)
            .encodeX("Weapon", Ordinal, sortOrder=vegas.spec.Spec.SortOrderEnums.None)
            .encodeY("Crimes Committed", Quantitative)
            .mark(Bar)
            .show

        // Line chart of total crimes per hour
        val hourlyRdd = seasonalDf.select("hour").rdd
            .map[(Int, Int)](x => (x(0).asInstanceOf[Int], 1))
            .reduceByKey(_ + _)
            .sortByKey()
            .collect
        tempDf = df.sqlContext.createDataFrame(hourlyRdd).withColumnRenamed("_1", "Hour").withColumnRenamed("_2", "Crimes Committed")
        Vegas("Total Crimes Per Hour")
            .withDataFrame(tempDf)
            .encodeX("Hour", Ordinal)
            .encodeY("Crimes Committed", Quantitative)
            .mark(Line)
            .show

        // Line chart of the total crimes per day of week (1 = Monday)
        val dailyRdd = seasonalDf.select("month", "day", "year").rdd
            .map(x => (new GregorianCalendar(x(2).asInstanceOf[Int], x(0).asInstanceOf[Int], x(1).asInstanceOf[Int]).get(Calendar.DAY_OF_WEEK), 1))
            .reduceByKey(_ + _)
            .map({ case (day, count) => (count, day)})
            .sortByKey()
            .collect
        tempDf = df.sqlContext.createDataFrame(dailyRdd).withColumnRenamed("_1", "Crimes Committed").withColumnRenamed("_2", "Day")
        Vegas("Total Crimes Per Day of Week (1 = Monday)")
            .withDataFrame(tempDf)
            .encodeX("Day", Ordinal)
            .encodeY("Crimes Committed", Quantitative)
            .mark(Line)
            .show

        // Line chart of total crimes each month Jan 2020-Dec 2021
        val monthlyRdd = seasonalDf.select("month", "year").select("*").where(seasonalDf.col("year") < 2022).rdd
            .map(x => ((x(0).asInstanceOf[Int], x(1).asInstanceOf[Int]), 1))
            .reduceByKey(_ + _)
            .map({ case ((month, year), count) => ((year, month), count)})
            .sortByKey().collect
            .map({ case ((year, month), count) => (year.toString + f"-$month%02d", count)})
            .toList
        tempDf = df.sqlContext.createDataFrame(monthlyRdd).withColumnRenamed("_1", "Month").withColumnRenamed("_2", "Crimes Committed")
        Vegas("Total Crimes Each Month Jan 2020-Dec 2021")
            .withDataFrame(tempDf)
            .encodeX("Month", Ordinal)
            .encodeY("Crimes Committed", Quantitative)
            .mark(Line)
            .show

        // Top 10 crimes by hour
        val hourlyTop10Rdd = seasonalDf.select("hour", "desc").rdd
            .map(x => ((x(0).asInstanceOf[Int], x(1).toString), 1))
            .filter({ case ((_, desc), _) => top10Crimes contains desc})
            .reduceByKey(_ + _)
            .map({ case ((hour, desc), count) => (hour, (desc, count))})
            .sortByKey()
            .map({ case (hour, (desc, count)) => (hour, desc, count)})
            .collect
        tempDf = df.sqlContext.createDataFrame(hourlyTop10Rdd).withColumnRenamed("_1", "Hour").withColumnRenamed("_2", "Type").withColumnRenamed("_3", "Crimes Committed")
        Vegas("Top 10 Types of Crimes Per Hour")
            .withDataFrame(tempDf)
            .encodeX("Hour", Ordinal)
            .encodeY("Crimes Committed", Quantitative)
            .encodeColor("Type", Nominal)
            .mark(Line)
            .show

        // Top 10 crimes each month Jan 2020-Dec 2021
        val monthlyTop10Rdd = seasonalDf.select("month", "year", "desc").select("*").where(seasonalDf.col("year") < 2022).rdd
            .map(x => ((x(0).asInstanceOf[Int], x(1).asInstanceOf[Int], x(2).toString), 1))
            .filter({ case ((_, _, desc), _) => top10Crimes contains desc})
            .reduceByKey(_ + _)
            .map({ case ((month, year, desc), count) => ((year, month), (desc, count))})
            .sortByKey().collect
            .map({ case ((year, month), (desc, count)) => (year.toString + f"-$month%02d", desc, count)})
            .toList
        tempDf = df.sqlContext.createDataFrame(monthlyTop10Rdd).withColumnRenamed("_1", "Date").withColumnRenamed("_2", "Type").withColumnRenamed("_3", "Crimes Committed")
        Vegas("Top 10 Types of Crimes Each Month Jan 2020-Dec 2021")
            .withDataFrame(tempDf)
            .encodeX("Date", Ordinal)
            .encodeY("Crimes Committed", Quantitative)
            .encodeColor("Type", Nominal)
            .mark(Line)
            .show

        // Top 10 weapons by hour
        val hourlyTop10WeaponsRdd = seasonalDf.select("hour", "weapon").rdd
            .map(x => ((x(0).asInstanceOf[Int], x(1).toString), 1))
            .filter({ case ((_, weapon), _) => top10Weapons contains weapon})
            .reduceByKey(_ + _)
            .map({ case ((hour, weapon), count) => (hour, (weapon, count))})
            .sortByKey()
            .map({ case (hour, (weapon, count)) => (hour, weapon, count)})
            .collect
        tempDf = df.sqlContext.createDataFrame(hourlyTop10WeaponsRdd).withColumnRenamed("_1", "Hour").withColumnRenamed("_2", "Weapon").withColumnRenamed("_3", "Crimes Committed")
        Vegas("Top 10 Weapons Used Per Hour")
            .withDataFrame(tempDf)
            .encodeX("Hour", Ordinal)
            .encodeY("Crimes Committed", Quantitative, scale=Scale(spec.Spec.ScaleTypeEnums.Log))
            .encodeColor("Weapon", Nominal)
            .mark(Line)
            .show

        // Top 10 weapons each month Jan 2020-Dec 2021
        val monthlyTop10WeaponsRdd = seasonalDf.select("month", "year", "weapon").select("*").where(seasonalDf.col("year") < 2022).rdd
            .map(x => ((x(0).asInstanceOf[Int], x(1).asInstanceOf[Int], x(2).toString), 1))
            .filter({ case ((_, _, weapon), _) => top10Weapons contains weapon})
            .reduceByKey(_ + _)
            .map({ case ((month, year, weapon), count) => ((year, month), (weapon, count))})
            .sortByKey().collect
            .map({ case ((year, month), (weapon, count)) => (year.toString + f"-$month%02d", weapon, count)})
            .toList
        tempDf = df.sqlContext.createDataFrame(monthlyTop10WeaponsRdd).withColumnRenamed("_1", "Date").withColumnRenamed("_2", "Weapon").withColumnRenamed("_3", "Crimes Committed")
        Vegas("Top 10 Weapons Used Each Month Jan 2020-Dec 2021")
            .withDataFrame(tempDf)
            .encodeX("Date", Ordinal)
            .encodeY("Crimes Committed", Quantitative, scale=Scale(spec.Spec.ScaleTypeEnums.Log))
            .encodeColor("Weapon", Nominal)
            .mark(Line)
            .show

        // Age each month Jan 2020-Dec 2021
        val ageByMonthRdd = seasonalDf.select("month", "year", "age").select("*").where(seasonalDf.col("year") < 2022).rdd
            .map(x => ((x(0).asInstanceOf[Int], x(1).asInstanceOf[Int], x(2).asInstanceOf[Int]), 1))
            .filter({ case ((_, _, age), _) => age > 0})
            .map({case ((month, year, age), count) => ((month, year, age
                match {
                    case x if x < 18 => "<18"
                    case x if 18 until 25 contains x => "18-25"
                    case x if 25 until 40 contains x => "25-40"
                    case x if 40 until 60 contains x => "40-60"
                    case x if x >= 60 => ">=60"
            }), count)})
            .reduceByKey(_ + _)
            .map({ case ((month, year, age), count) => ((year, month), (age, count))})
            .sortByKey().collect
            .map({ case ((year, month), (age, count)) => (year.toString + f"-$month%02d", age, count)})
            .toList
        tempDf = df.sqlContext.createDataFrame(ageByMonthRdd).withColumnRenamed("_1", "Month").withColumnRenamed("_2", "Age").withColumnRenamed("_3", "Crimes Committed")
        Vegas("Total Crimes by Age Range Jan 2020-Dec 2021")
            .withDataFrame(tempDf)
            .encodeX("Month", Ordinal)
            .encodeY("Crimes Committed", Quantitative)
            .encodeColor("Age", Nominal)
            .mark(Line)
            .show

        // Gender each month Jan 2020-Dec 2021
        val genderByMonthRdd = seasonalDf.select("month", "year", "gender").select("*").where(seasonalDf.col("year") < 2022).rdd
            .map(x => ((x(0).asInstanceOf[Int], x(1).asInstanceOf[Int], x(2).toString), 1))
            .filter({ case ((_, _, gender), _) => (gender == "F") || (gender == "M")})
            .reduceByKey(_ + _)
            .map({ case ((month, year, gender), count) => ((year, month), (gender, count))})
            .sortByKey().collect
            .map({ case (monthYear, (gender, count)) => (monthYear._1.toString + f"-${monthYear._2}%02d", gender, count)})
            .toList
        tempDf = df.sqlContext.createDataFrame(genderByMonthRdd).withColumnRenamed("_1", "Month").withColumnRenamed("_2", "Gender").withColumnRenamed("_3", "Crimes Committed")
        Vegas("Total Crimes by Gender Jan 2020-Dec 2021")
            .withDataFrame(tempDf)
            .encodeX("Month", Ordinal)
            .encodeY("Crimes Committed", Quantitative)
            .encodeColor("Gender", Nominal)
            .mark(Line)
            .show
    }
}
