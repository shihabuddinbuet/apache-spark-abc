package com.su.apache.spark.basic

import com.su.apache.spark.utils.Constant.Student
import org.apache.spark.sql.SparkSession

object AggregateDataframe {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession
      .builder()
      .master("local")
      .appName("dataframe aggregation")
      .getOrCreate()

    import spark.implicits._

    val studentDf = Seq(
      Student(1,1,"Emil", 20, "dhaka"),
      Student(2,3, "Pinar", 22, "london"),
      Student(3,3, "Anderson", 21, "dhaka"),
      Student(4,3, "Khan", 20, "paris"),
      Student(5,3, "Adeep", 23, "london"),
      Student(6,3, "lazzlie", 22, "delhi"),
      Student(7,3, "clarke", 20, "london")
    ).toDF("id", "d_id", "name", "age", "address")

    val avgAge = studentDf
        .groupBy()
        .avg("age")
    avgAge.show()


    val stdZonewiseCount = studentDf
          .groupBy("address")
          .count().as("count")
    stdZonewiseCount.show()

    val oldestStd = studentDf.groupBy().max("age")
    oldestStd.show()

  }
}
