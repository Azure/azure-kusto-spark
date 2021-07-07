package com.microsoft.kusto.spark.utils

case class RetriesExhaustedException(msg: String) extends scala.Exception(msg) {

}
