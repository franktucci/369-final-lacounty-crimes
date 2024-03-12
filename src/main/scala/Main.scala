import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}

object Main
{
    def main(args: Array[String]): Unit =
    {
        Logger.getLogger("org").setLevel(Level.OFF)
        Logger.getLogger("akka").setLevel(Level.OFF)
        val conf = new SparkConf().setAppName("369FINALPROJECT").setMaster("local[4]")
        val sc = new SparkContext(conf)
        val spark = SparkSession.builder().getOrCreate()

        // Reading data into a spark DataFrame. These are kind of hard to work with as there isn't much
        // documentation available, but there are some cool data visualization things with vegas-viz (and
        // mllib maybe, but I haven't checked)
        val df = spark.read
            .format("csv")
            .option("header", "true")
            .option("inferSchema", "true")
            .load("data/Crime_Data_from_2020_to_Present.csv")
        df.show()

        // Converts a spark DataFrame to an RDD
        val data: RDD[Row] = df.rdd

        // Sorting crimes by date reported (just an example)
        val sortedRDD = data.keyBy(_(1).asInstanceOf[String])
            .map({case (dateTime, row) =>
                val tokens = dateTime.split(" ")(0).split("/")
                (tokens(2) + "-" + tokens(0) + "-" + tokens(1), row)
            })
            .sortByKey()
            .map(_._2)

        // Collect sorted values back into a new dataframe
        val dfSorted = df.sqlContext.createDataFrame(sortedRDD, df.schema)
        dfSorted.show()
    }
}