package com.microsoft.kusto.spark.utils

import java.util.function.Predicate

object JavaConverter {
  def asJavaPredicate[T](p:(T)=> Boolean): Predicate[T] ={
    new Predicate[T] {
      override def test(t: T): Boolean = p(t)
    }
  }
}
