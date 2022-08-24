package org.example.custom.source

import org.apache.flink.api.connector.source.{SplitEnumerator, SplitEnumeratorContext}

import java.util

class IntEnumerator(context:SplitEnumeratorContext[IntRangeSplit],  state: EnumeratorState) extends SplitEnumerator[IntRangeSplit, EnumeratorState]{

  def this(context:SplitEnumeratorContext[IntRangeSplit]) = {
    this(context, EnumeratorState(0))
  }

  override def start(): Unit = {}

  override def handleSplitRequest(subtaskId: Int, requesterHostname: String): Unit = {
    val from = state.currentValue
    val until = state.currentValue + 1000
    context.assignSplit(new IntRangeSplit(from, until), subtaskId)
    state.currentValue = until
  }

  override def addSplitsBack(splits: util.List[IntRangeSplit], subtaskId: Int): Unit = {}

  override def addReader(subtaskId: Int): Unit = {}

  override def snapshotState(checkpointId: Long): EnumeratorState = state

  override def close(): Unit = ???
}
