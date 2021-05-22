package ru.noit.job

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DoubleType, StringType, StructType}
import ru.noit.utils.Constants._

import scala.util.Try

object RawDataHandlerJob {

  def main(args: Array[String]): Unit = {

    val readFromArg = Try { args(0) }
    val writeFromArg = Try { args(1) }

    val readFrom = if (readFromArg.isSuccess) readFromArg.get else throw new IllegalStateException("Path for reading file is mandatory parameter")
    val writeTo = if (writeFromArg.isSuccess) readFromArg.get else BASE_WRITE_PATH

    val spark = SparkSession
      .builder
      .appName("WeatherRP5Raw")
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

    val initialData = spark
      .read
      .option("delimiter", ";")
      .schema(weatherSchema)
      .csv(readFrom)

    initialData
      .write
      .option("header", "true")
      .csv(writeTo)
  }
}
