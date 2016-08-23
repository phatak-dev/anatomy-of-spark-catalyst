package org.apache.spark.sql.core.phyzicalplan

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, BoundReference, EqualTo, Literal}
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, Project}
import org.apache.spark.sql.execution.{LogicalRDD, PhysicalRDD}
import org.apache.spark.sql.types.IntegerType

/**
 * Created by madhu on 23/07/16.
 */

object SparkStrategyExample {

  def main(args: Array[String]): Unit = {

    val sparkContext = new SparkContext("local","test")

    val sqlContext = new SQLContext(sparkContext)

    val attributeReference = AttributeReference("id",IntegerType)()
    val booleanExpression = EqualTo(attributeReference,Literal(10))

    val data =List(10,2).map(value => InternalRow.apply(value))

    val attributes = Seq(attributeReference)

    val localRelation = LocalRelation(attributes,data)

    val filterLogicalPlan = org.apache.spark.sql.catalyst.plans.logical.Filter(booleanExpression,localRelation)

    SQLContext.setActive(sqlContext)
    val planner = sqlContext.planner

    //basic operators for spark conversion
    val basicOperators = planner.BasicOperators 

    //spark plan for local relation
    val localTableScan = basicOperators(localRelation)
    println(localTableScan.head)

    val filterSparkPlan = basicOperators(filterLogicalPlan)
    println(filterSparkPlan.head)

  }
}
