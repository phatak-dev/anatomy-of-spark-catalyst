package com.madhukaraphatak.spark.catalyst.expressions

import java.io.{ByteArrayOutputStream, ObjectOutputStream}

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{SpecificMutableRow, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.types.{DataType, DoubleType, IntegerType}

/**
  * Created by madhu on 19/07/16.
  */
object InternalRowExample {

  def main(args: Array[String]): Unit = {

    val simpleArray = Array(1,2.0)

    val row = Row.fromSeq(simpleArray)

    //convert row to Internal Row

    val internalRow = InternalRow.fromSeq(row.toSeq)
    println(internalRow.getClass.getSimpleName)

    val bos = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(bos)
    oos.writeObject(row)
    println("Normal row byte array length " + bos.toByteArray.length)

    // work with UnsafeRow

    val fieldTypes: Array[DataType] = Array(IntegerType, DoubleType)
    val mutableRow = new SpecificMutableRow(fieldTypes)
    mutableRow.setInt(0,1)
    mutableRow.setDouble(1,2.0)

    val converter = UnsafeProjection.create(fieldTypes)
    val unsafeRow = converter.apply(mutableRow)
    println("Unsafe row byte array length " + unsafeRow.getBytes.length)
  }

}
