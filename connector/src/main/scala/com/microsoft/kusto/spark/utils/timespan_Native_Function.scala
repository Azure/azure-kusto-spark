package org.apache.spark.sql.catalyst.expressions
import org.apache.commons.lang3.time.DurationFormatUtils
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.types.{DataType, StringType}
import org.apache.spark.unsafe.types.UTF8String

case class timespan_Native_Function(child: Expression) extends UnaryExpression with NullIntolerant {
  override def dataType: DataType = StringType
  override def prettyName: String = "addOneSparkNative"
  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = defineCodeGen(ctx, ev, c => s"($c/10000000/3600/24).toString()")
  override def nullSafeEval(input: Any): Any = {
    if (input == null) null else {
      val longVal = input.asInstanceOf[Long]
      val nanoSuffix = longVal % 10000000
      UTF8String.fromString(DurationFormatUtils.formatDuration(longVal / 10000L,
        s"d:HH:mm:ss${if(nanoSuffix > 0) ".".concat(nanoSuffix.toString) else ""}"))
      //UTF8String.fromString((input.asInstanceOf[Long] / 10000000 / 3600 / 24).toString)
    }
  }
}