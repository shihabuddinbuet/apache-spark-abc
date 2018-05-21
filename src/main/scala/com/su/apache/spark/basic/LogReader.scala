package com.su.apache.spark.basic

import com.databricks.spark.avro.AvroDataFrameWriter
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}


object LogReader {

  val CSV_DATA_PATH = "src/main/resources/data/play.csv"
  val TSV_DATA_PATH = "src/main/resources/data/play.tsv"
  val JSON_DATA_PATH = "src/main/resources/data/play.json"
  val PARQUET_DATA_PATH="src/main/resources/data/play.snappy.parquet"
  val AVRO_DATA_PATH = "src/main/resources/data/play.avro"

  val SCHEMA = StructType(Array(
    StructField("temperature", StringType, true),
    StructField("outlook", StringType, true),
    StructField("humidity", StringType, true),
    StructField("windy", StringType, true),
    StructField("play", StringType, true)
  ))

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession
          .builder()
          .appName("reading logs")
          .master("local")
          .getOrCreate()

    println("**************Reading CSV file*************")
    val csv_data = spark
        .read
        .format("csv")
        .option("sep", ",")
        .schema(SCHEMA)
        .csv(CSV_DATA_PATH)

    csv_data.show()

    println("**************Reading TSV file*************")
    val tsv_data = spark
        .read
        .format("csv")
        .option("sep", "\t")
        .schema(SCHEMA)
        .csv(TSV_DATA_PATH)

    tsv_data.show()

    println("**************Reading JSON file************")
    val jsn_data = spark
        .read
        .format("json")
        .json(JSON_DATA_PATH)

    jsn_data.show()

    println("*****************Reading parquet file******************")
    val parquet_data = spark
        .read
        .parquet(PARQUET_DATA_PATH)

    parquet_data.show()

    println("*****************Reading avro file**********************")
    val avro_data = spark
        .read
        .format("com.databricks.spark.avro")
        .load(AVRO_DATA_PATH)

    avro_data.show()

  }
}
