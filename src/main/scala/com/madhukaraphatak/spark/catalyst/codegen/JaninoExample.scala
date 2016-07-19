package com.madhukaraphatak.spark.catalyst.codegen

import org.apache.spark.sql.catalyst.InternalRow
import org.codehaus.janino.ClassBodyEvaluator

/**
  * Created by madhu on 13/07/16.
  */
object JaninoExample {

  def main(args: Array[String]) {
    val classBodyEvaluator = new ClassBodyEvaluator()
    classBodyEvaluator.setImplementedInterfaces(Array(classOf[ExpressionEvaluator]))
    classBodyEvaluator.setClassName("org.apache.madhukaraphatak.spark.Expression")
    classBodyEvaluator.setDefaultImports(Array(classOf[InternalRow].getName))
    val string =
      """
        |public Object evaluate(InternalRow value) {
        |    double returnValue = value.getDouble(0) + value.getDouble(1);
        |    System.out.println(returnValue);
        |    return returnValue;
        |  }
      """.stripMargin

    classBodyEvaluator.cook(string)

    val evaluatorInstance = classBodyEvaluator.getClazz.newInstance().asInstanceOf[ExpressionEvaluator]
    evaluatorInstance.evaluate(InternalRow(5.0,7.0))
  }

}
