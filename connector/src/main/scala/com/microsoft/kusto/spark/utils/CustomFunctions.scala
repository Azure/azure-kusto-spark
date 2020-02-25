package org.apache.spark.sql
import org.apache.spark.sql.catalyst.expressions._
object CustomFunctions {
  private def withExpr(expr: Expression): Column = Column(expr)

  def timespanNativeFunction(x: Column): Column = withExpr {
    timespan_Native_Function(x.expr)
  }
}