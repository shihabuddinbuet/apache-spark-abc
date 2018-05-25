package com.su.apache.spark.utils

object Constant {

  case class Student(id:Int, department_id : Int, name:String, age:Int, address:String)
  case class Department(id : Int, name: String)

}
