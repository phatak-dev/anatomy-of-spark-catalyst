package com.madhukaraphatak.spark.catalyst.expressions

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Add, Cos, Literal, UnaryMinus}

/**
  * Created by madhu on 18/07/16.
  */
object SimpleExpressions {

  def main(args: Array[String]){

    val inputRow:InternalRow = null

    //simple literals - leaf expression

    val literalExpression = Literal(10)

    println(literalExpression.eval(inputRow))

    // Unary expression

    val unaryMinus = UnaryMinus(Literal(10))
    println(unaryMinus.eval(inputRow))

    // ADD expression

    val literalADD = Add(Literal(10),Literal(20))
    println(literalADD.eval(inputRow))

    //Math expression
    val cos = Cos(Literal(10.0))
    println(cos.eval(inputRow))


  }

}
