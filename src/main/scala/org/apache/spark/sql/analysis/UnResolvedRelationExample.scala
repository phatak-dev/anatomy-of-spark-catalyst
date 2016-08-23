package org.apache.spark.sql.analysis

import org.apache.spark.sql.catalyst.analysis.{Analyzer, EliminateSubQueries, SimpleCatalog, UnresolvedRelation}
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation
import org.apache.spark.sql.catalyst.{SimpleCatalystConf, TableIdentifier}
import org.apache.spark.sql.types.IntegerType

/**
 * Created by madhu on 24/07/16.
 */
object UnResolvedRelationExample {

  def main(args: Array[String]): Unit = {
    val attributeReference = AttributeReference("id",IntegerType)()
    val attributes = Seq(attributeReference)
    val localRelation = LocalRelation(attributes)

    //understanding non analyzed plan in spark sql
    val tableIdentifier = TableIdentifier("sales")
    val unresolvedRelation = UnresolvedRelation(tableIdentifier)
    println("unresolved plan is")
    println(unresolvedRelation.numberedTreeString)

    val catalog = new SimpleCatalog(new SimpleCatalystConf(true))
    catalog.registerTable(tableIdentifier, localRelation)
    println(catalog.tableExists(tableIdentifier))

    val analyser = new Analyzer(catalog,null,new SimpleCatalystConf(true))
    val resolvedPlan =analyser.execute(unresolvedRelation)
    println("resolved plan is")
    println(resolvedPlan.numberedTreeString)

    //use optimizer subsquery removal to improve
    val subqueryTransformerPlan = EliminateSubQueries(resolvedPlan)
    println("after removing the subquery")
    println(subqueryTransformerPlan.numberedTreeString)
  }
}
