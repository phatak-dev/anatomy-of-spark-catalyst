package com.madhukaraphatak.spark.catalyst.codegen;

import org.apache.spark.sql.catalyst.InternalRow;

/**
 * Created by madhu on 13/07/16.
 */
public interface ExpressionEvaluator {
    Object evaluate(InternalRow row);
}
