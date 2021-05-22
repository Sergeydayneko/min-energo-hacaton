package ru.noit.job

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types.{DoubleType, StringType, StructType}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import ru.noit.utils.Constants._

import scala.util.Try

object MatchingWithTownAndDateTimeJob {

  def main(args: Array[String]) {

    val readFromArg = Try { args(0) }
    val writeFromArg = Try { args(1) }
    val stepFromArg = Try { args(2) }

    val readFrom = if (readFromArg.isSuccess) readFromArg.get else throw new IllegalStateException("Path for reading file is mandatory parameter")
    val writeTo = if (writeFromArg.isSuccess) readFromArg.get else BASE_WRITE_PATH
    val step = if (stepFromArg.isSuccess) stepFromArg.get else BASE_STEP

    val spark = SparkSession
      .builder
      .appName("WeatherMatchingByDateAndTime")
      .master("local[*]")
      .getOrCreate()

    Logger.getLogger("org").setLevel(Level.ERROR)

    val weatherSchema = new StructType()
      .add("date", StringType, nullable = true)
      .add("temperature", DoubleType, nullable = true)
      .add("pressure_at_station", DoubleType, nullable = true)
      .add("pressure_avg_to_sea", DoubleType, nullable = true)
      .add("relative_humidity", DoubleType, nullable = true)
      .add("mean_wind_direction", StringType, nullable = true)
      .add("mean_wind_speed", StringType, nullable = true)
      .add("maximum_gust_value", StringType, nullable = true)
      .add("maximum_gust_value_all_periods", StringType, nullable = true)
      .add("maximum_gust_value_all_periods_observations", StringType, nullable = true)
      .add("total_cloud_cover", StringType, nullable = true)
      .add("weather_reported", StringType, nullable = true)
      .add("past_weather", StringType, nullable = true)
      .add("past_weather_of_period", StringType, nullable = true)
      .add("min_air_temperature", StringType, nullable = true)
      .add("max_air_temperature", StringType, nullable = true)
      .add("clouds_of_genera_cumulus", StringType, nullable = true)
      .add("amount_of_CL_cloud", StringType, nullable = true)
      .add("height_of_clouds", StringType, nullable = true)
      .add("clouds_of_genera_altocumulus", StringType, nullable = true)
      .add("clouds_of_genera_cirrus", StringType, nullable = true)
      .add("horizontal_visibility", DoubleType, nullable = true)
      .add("dew_point_temperature_2m", DoubleType, nullable = true)
      .add("amount_of_precipitation", StringType, nullable = true)
      .add("time_of_amount_of_precipitation", DoubleType, nullable = true)
      .add("state_of_ground", DoubleType, nullable = true)

    val dateBaseSchema = new StructType()
      .add("date", StringType, nullable = false)

    val timesSeriesFrame = spark
      .read
      .schema(dateBaseSchema)
      .csv(readFrom)

    val dateConversionToRP5UDF = udf(convertDateFormatRP5 _)
    val convertDateFormatWithTruncatedHourUDF = udf(convertDateFormatWithTruncated1Hour _)
    val convertDateFormatWithTruncatedThreeHourUDF = udf(convertDateFormatWithTruncated3Hour _)

    val neededTimeSeries: DataFrame =
      if (step == "3") {
        timesSeriesFrame
          .select(col("date"))
          .sort(col("date").desc_nulls_last)
          .withColumn("date_conversion_to_RP5", dateConversionToRP5UDF(col("date")))
          .withColumn("date_with_truncated_hour_format", convertDateFormatWithTruncatedThreeHourUDF(col("date")))
          .select(
            col("date").as("raw_date"),
            col("date_conversion_to_RP5"),
            col("date_with_truncated_hour_format")
          )
      } else {
        timesSeriesFrame
          .select(col("date"))
          .sort(col("date").desc_nulls_last)
          .withColumn("date_conversion_to_RP5", dateConversionToRP5UDF(col("date")))
          .withColumn("date_with_truncated_hour_format", convertDateFormatWithTruncatedHourUDF(col("date")))
          .select(
            col("date").as("raw_date"),
            col("date_conversion_to_RP5"),
            col("date_with_truncated_hour_format")
          )
      }

    val dateForTimeSeries: DataFrame = spark
      .read
      .option("delimiter", ";")
      .schema(weatherSchema)
      .csv("./data/datalake/maikop-2020.csv")

    neededTimeSeries
      .join(dateForTimeSeries,
        neededTimeSeries("date_with_truncated_hour_format") === dateForTimeSeries("date"),
        "inner")
      .select(
        col("raw_date").as("date"),
        col("temperature"),
        col("pressure_at_station"),
        col("pressure_avg_to_sea"),
        col("relative_humidity"),
        col("mean_wind_direction"),
        col("mean_wind_speed"),
        col("maximum_gust_value"),
        col("maximum_gust_value_all_periods"),
        col("maximum_gust_value_all_periods_observations"),
        col("total_cloud_cover"),
        col("weather_reported"),
        col("past_weather"),
        col("past_weather_of_period"),
        col("min_air_temperature"),
        col("max_air_temperature"),
        col("clouds_of_genera_cumulus"),
        col("amount_of_CL_cloud"),
        col("height_of_clouds"),
        col("clouds_of_genera_altocumulus"),
        col("clouds_of_genera_cirrus"),
        col("horizontal_visibility"),
        col("dew_point_temperature_2m"),
        col("amount_of_precipitation"),
        col("time_of_amount_of_precipitation"),
        col("state_of_ground"))
      .write
      .option("header", "true")
      .mode(SaveMode.Overwrite)
      .csv(writeTo)
  }

  def convertDateFormatRP5(dateAsString: String): String = {

    val dateAndTime: Array[String] = dateAsString.split(" ")

    val reversedDate = dateAndTime(0)
      .split("-")
      .reverse
      .mkString(".")

    val roundedTime = dateAndTime(1)
      .split(":")
      .take(2)
      .mkString(":")

    s"""$reversedDate $roundedTime"""
  }

  def convertDateFormatWithTruncated1Hour(dateAsString: String): String = {
    val dateAndTime: Array[String] = dateAsString.split(" ")

    val reversedDate = dateAndTime(0)
      .split("-")
      .reverse
      .mkString(".")

    val hourToMinutes = dateAndTime(1).split(":")

    val hour = hourToMinutes(0)
    val minutes = "00"

    s"""$reversedDate $hour:$minutes"""
  }

  def convertDateFormatWithTruncated3Hour(dateAsString: String): String = {
    val dateAndTime: Array[String] = dateAsString.split(" ")

    val reversedDate = dateAndTime(0)
      .split("-")
      .reverse
      .mkString(".")

    val hourToMinutes = dateAndTime(1).split(":")

    val hour = hourToMinutes(0) match {
      case "01" | "02" => "03"
      case "04" | "05"  => "06"
      case "07" => "06"
      case "08" => "09"
      case "10" => "09"
      case "11" => "12"
      case "13" | "14" => "15"
      case "16" | "17" => "15"
      case "19" => "18"
      case "20" => "21"
      case "22" | "23" => "00"
      case correctNumber: String => correctNumber
    }

    val truncatedMinutes = "00"

    s"""$reversedDate $hour:$truncatedMinutes"""
  }
}
