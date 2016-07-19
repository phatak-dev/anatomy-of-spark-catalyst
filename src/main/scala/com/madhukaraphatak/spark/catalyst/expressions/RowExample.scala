package com.madhukaraphatak.spark.catalyst.expressions

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.catalyst.expressions.{GenericRowWithSchema, MutableRow}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

/**
  * Created by madhu on 19/07/16.
  */
object RowExample {

  def main(args: Array[String]): Unit = {

    //create a Row from array
    val values = Array(1,"hi","how")
    val row = Row.fromSeq(values)
    println(row.getClass.getSimpleName)

    //GenericRow with Schema
    val schema = StructType(Array(StructField("_1",IntegerType),StructField("_2",StringType),StructField("_3",StringType)))
    val genericRowWithSchema = new GenericRowWithSchema(values,schema)
    println(genericRowWithSchema.schema)

  }

}
