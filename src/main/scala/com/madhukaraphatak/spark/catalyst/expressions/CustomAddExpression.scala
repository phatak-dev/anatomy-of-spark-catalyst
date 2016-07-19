package com.madhukaraphatak.spark.catalyst.expressions

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.types.{DataType, DoubleType}

/**
  * Created by madhu on 9/7/16.
  */
object CustomAddExpression {

   case class CustomAddExpression(left:Expression,right: Expression) extends BinaryExpression with CodegenFallback{
     override def nullable: Boolean = false

     override def dataType: DataType = DoubleType

     override def eval(input: InternalRow): Any = {
       val leftValue = left.eval(input).asInstanceOf[Double]
       val rightValue = right.eval(input).asInstanceOf[Double]

       leftValue+rightValue
     }

     override def prettyName: String = "customAdd"

   }


  def main(args: Array[String]) {
    val sqlContext = new SQLContext(new SparkContext("local","test"))

    val rdd = sqlContext.sparkContext.makeRDD(List(("hi",5.0),("hell0",7.0)))

    val dataFrame = sqlContext.createDataFrame(rdd)

    val customAddExpression = new CustomAddExpression(BoundReference(1,DoubleType,true),Literal(2.0))

    val dfWithToString = dataFrame.withColumn(Column(customAddExpression))

    dfWithToString.explain()

    dfWithToString.show()



  }

}
