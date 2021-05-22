package ru.noit.job

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructType}
import ru.noit.utils.Constants.BASE_WRITE_PATH

import scala.util.Try

object TimeSeriesWithRoundHours {

  def main(args: Array[String]): Unit = {

    val readFromArg = Try { args(0) }
    val writeFromArg = Try { args(1) }

    val readFrom = if (readFromArg.isSuccess) readFromArg.get else throw new IllegalStateException("Path for reading file is mandatory parameter")
    val writeTo = if (writeFromArg.isSuccess) readFromArg.get else BASE_WRITE_PATH

    val spark = SparkSession
      .builder
      .appName("TimeSeriesWithRoundHours")
      .master("local[*]")
      .getOrCreate()

    Logger.getLogger("org").setLevel(Level.ERROR)

    spark
      .read
      .schema(new StructType().add("date", StringType, nullable = false))
      .csv(readFrom)
      .select(to_timestamp(col("date")).as("date_time"))
      .withColumn("day_of_year", dayofyear(col("date_time")))
      .withColumn("hour", hour(col("date_time")))
      .withColumn("minute", minute(col("date_time")))
      .sort(
        col("day_of_year"),
        col("hour"),
        col("minute"))
      .select(col("date_time"))
      .withColumn("date_time_round", date_trunc("HOUR", col("date_time")))
      .withColumn("date_formatted", to_date(col("date_time_round"), "dd-MM-yyyy HH:mm"))
      .write
      .csv(writeTo)
  }
}
