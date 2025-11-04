// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.kusto.spark.exceptions

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.ListBuffer

/**
 * The 3 methods below are adapted from Apache Commons Lang3 ExceptionUtils class, licensed under
 * Apache License 2.0
 */
object ExceptionUtils {
  private val NOT_FOUND = -1
  private val WRAPPED_MARKER = " [wrapped] "

  def getRootCauseStackTrace(throwable: Throwable): Array[String] = {
    getRootCauseStackTraceList(throwable).toArray
  }

  def getRootCauseStackTraceList(throwable: Throwable): Seq[String] = {
    if (throwable == null) return Seq.empty
    val throwables = getThrowables(throwable)
    val count = throwables.length
    val frames = ListBuffer[String]()
    var nextTrace = getStackFrameList(throwables(count - 1))
    for (i <- (0 until count).reverse) {
      val trace = nextTrace
      if (i != 0) {
        nextTrace = getStackFrameList(throwables(i - 1))
        removeCommonFrames(trace, nextTrace)
      }
      if (i == count - 1) {
        frames += throwables(i).toString
      } else {
        frames += (WRAPPED_MARKER + throwables(i).toString)
      }
      frames ++= trace
    }
    frames.toSeq
  }

  def getStackFrameList(throwable: Throwable): Seq[String] = {
    val stackTrace = getStackTrace(throwable)
    val linebreak = System.lineSeparator()
    val frames = stackTrace.split(linebreak)
    val list = ListBuffer[String]()
    var traceStarted = false
    for (token <- frames) {
      val at = token.indexOf("at")
      if (at != NOT_FOUND && token.substring(0, at).trim.isEmpty) {
        traceStarted = true
        list += token
      } else if (traceStarted) {
        // Stop after stack frames
        return list.toSeq
      }
    }
    list.toSeq
  }

  def getStackTrace(throwable: Throwable): String = {
    if (throwable == null) ""
    else {
      val sw = new java.io.StringWriter()
      throwable.printStackTrace(new java.io.PrintWriter(sw, true))
      sw.toString
    }
  }

  private def getThrowables(throwable: Throwable): Array[Throwable] = {
    getThrowableList(throwable).toArray
  }

  private def getThrowableList(throwable: Throwable): List[Throwable] = {
    val list = ListBuffer[Throwable]()
    var t = throwable
    while (t != null && !list.contains(t)) {
      list += t
      t = t.getCause
    }
    list.toList
  }

  private def removeCommonFrames(causeFrames: Seq[String], wrapperFrames: Seq[String]): Unit = {
    var causeFrameIndex = causeFrames.length - 1
    var wrapperFrameIndex = wrapperFrames.length - 1
    while (causeFrameIndex >= 0 && wrapperFrameIndex >= 0) {
      val causeFrame = causeFrames(causeFrameIndex)
      val wrapperFrame = wrapperFrames(wrapperFrameIndex)
      if (causeFrame == wrapperFrame) {
        causeFrames match {
          case lb: ListBuffer[String] => lb.remove(causeFrameIndex)
          case ab: ArrayBuffer[String] => ab.remove(causeFrameIndex)
          case _ => // immutable, do nothing
        }
      }
      causeFrameIndex -= 1
      wrapperFrameIndex -= 1
    }
  }

  def getRootCause(throwable: Throwable): Throwable = {
    val list = getThrowableList(throwable)
    if (list.isEmpty) null
    else list.lastOption.orNull
  }
}
