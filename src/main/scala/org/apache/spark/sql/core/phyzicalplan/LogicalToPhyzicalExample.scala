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

object LogicalToPhyzicalExample {

  def main(args: Array[String]): Unit = {

    val sparkContext = new SparkContext("local","test")
    val sqlContext = new SQLContext(sparkContext)

    val attributeReference = AttributeReference("id",IntegerType)()
    val booleanExpression = EqualTo(attributeReference,Literal(10))

    val data =List(10,2).map(value => InternalRow.apply(value))
    val attributes = Seq(attributeReference)
    val localRelation = LocalRelation(attributes,data)
    val filterLogicalPlan = org.apache.spark.sql.catalyst.plans.logical.Filter(booleanExpression,localRelation)

    //create phyzical plan
    SQLContext.setActive(sqlContext)
    val planner = sqlContext.planner
    val phyzicalPlans = planner.plan(filterLogicalPlan)
    val executedPlan =  phyzicalPlans.map(plan => plan.execute).toList.head
    val result = executedPlan.collect.toList
    println(result)
  }

}
