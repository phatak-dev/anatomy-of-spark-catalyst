package com.madhukaraphatak.spark.catalyst.codegen

import org.apache.spark.sql.catalyst.expressions.codegen.CodeGenContext
import org.apache.spark.sql.catalyst.expressions.{BoundReference, Expression, Literal, Multiply}
import org.apache.spark.sql.types.DoubleType

/**
  * Created by madhu on 13/07/16.
  */
object ExpressionCodeGenExample {

  def getCode(expression:Expression)(implicit codeGenContext: CodeGenContext)= {
    codeGenContext.generateExpressions(Seq(expression)).head
  }

  def main(args: Array[String]) {

    implicit val codeGenContext = new CodeGenContext()

    val addExpression = Multiply(Literal(1.0),Literal(2.0))
    println("code generated for add expression is ")
    println(getCode(addExpression).code)


    val boundReference = BoundReference(0,DoubleType,true)
    println("code generated for add expression is")
    println(getCode(boundReference).code)


  }

}
