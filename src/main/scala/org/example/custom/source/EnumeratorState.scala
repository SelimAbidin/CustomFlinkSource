package org.example.custom.source

case class EnumeratorState(var currentValue: Int, var deadSplits: List[IntRangeSplit] = List())
