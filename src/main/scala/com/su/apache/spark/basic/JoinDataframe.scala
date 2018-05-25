package com.su.apache.spark.basic

import com.su.apache.spark.utils.Constant.{Department, Student}
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions.{col}

object JoinDataframe {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession
        .builder()
        .master("local")
        .appName("dataframe joining")
        .getOrCreate()

    import spark.implicits._

    val studentDf = Seq(
      Student(1,1,"Emil", 20, "dhaka"),
      Student(2,3, "james", 22, "london")
    ).toDF("id", "d_id", "name", "age", "address")

    val departmentDf = Seq(
      Department(1, "English"),
      Department(2, "Math"),
      Department(3, "History")
    ).toDF("id", "name")

    println("Inner join between student and department")
    val stdDepInner = studentDf.join(departmentDf,studentDf("d_id") === departmentDf("id"), "inner")
                  .select(
                    studentDf("id").alias("s_id"),
                    studentDf("d_id"),
                    studentDf("name"),
                    departmentDf("name").alias("d_name"),
                    col("age"),
                    col("address")
                  )
    stdDepInner.show()

    println("Left outer join between student and department")
    val stdDepLeft = studentDf.join(departmentDf,studentDf("d_id") === departmentDf("id"), "left")
      .select(
        studentDf("id").alias("s_id"),
        studentDf("d_id"),
        studentDf("name"),
        departmentDf("name").alias("d_name"),
        col("age"),
        col("address")
      )
    stdDepLeft.show()

    println("Right outer join between student and department")
    val stdDepRight = studentDf.join(departmentDf,studentDf("d_id") === departmentDf("id"), "right")
      .select(
        studentDf("id").alias("s_id"),
        studentDf("d_id"),
        studentDf("name"),
        departmentDf("name").alias("d_name"),
        col("age"),
        col("address")
      )
    stdDepRight.show()

    println("Full outer join between student and department")
    val stdDepOuter = studentDf.join(departmentDf,studentDf("d_id") === departmentDf("id"), "outer")
      .select(
        studentDf("id").alias("s_id"),
        studentDf("d_id"),
        studentDf("name"),
        departmentDf("name").alias("d_name"),
        col("age"),
        col("address")
      )
    stdDepOuter.show()

    stdDepRight.filter(col("s_id").isNotNull).show()
    stdDepRight.filter("age > 20").show()
    stdDepRight.filter("name != 'Emil'").show()
    stdDepRight.filter("name != 'Emil' AND age > 17").show()
  }
}
