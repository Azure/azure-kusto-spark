package com.microsoft.kusto.spark.exceptions

case class NoStorageContainersException(msg: String) extends scala.Exception(msg) {}
