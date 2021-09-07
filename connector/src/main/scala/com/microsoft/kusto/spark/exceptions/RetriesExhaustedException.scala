package com.microsoft.kusto.spark.exceptions

case class RetriesExhaustedException(msg: String) extends scala.Exception(msg) {

}
