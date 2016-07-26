package com.madhukaraphatak.spark.catalyst.expressions

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Add, Alias, AttributeReference, BoundReference, ExprId, Literal}
import org.apache.spark.sql.types.{IntegerType, StringType}
import org.apache.spark.unsafe.types.UTF8String

/**
  * Created by madhu on 19/07/16.
  */
object ComplexExpressions {

  def main(args: Array[String]): Unit = {

    val values = Array(1,UTF8String.fromString("hi"),UTF8String.fromString("how"))
    val internalRow = InternalRow.fromSeq(values)

    //val boundreference
    val boundReferenceExpression = BoundReference(0, IntegerType,false)
    println("select value at reference is " +boundReferenceExpression.eval(internalRow))
    //ADD using boundreference
    val addExpression = Add(boundReferenceExpression,Literal(10))
    println(addExpression.eval(internalRow))

    //Alias expression
    val aliasExpression = Alias(addExpression,"addedValue")()
    println(aliasExpression.numberedTreeString)

  }

}
