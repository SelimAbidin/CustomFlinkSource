package org.example.custom.source

import org.apache.flink.api.connector.source.{SplitEnumerator, SplitEnumeratorContext}

import java.util
import scala.collection.JavaConverters._

class IntEnumerator(context: SplitEnumeratorContext[IntRangeSplit], state: EnumeratorState) extends SplitEnumerator[IntRangeSplit, EnumeratorState] {

  def this(context: SplitEnumeratorContext[IntRangeSplit]) = {
    this(context, EnumeratorState(0))
  }

  override def start(): Unit = {}

  override def handleSplitRequest(subtaskId: Int, requesterHostname: String): Unit = {
    // returned splits are prioritized
    if (state.deadSplits.size > 0) {
      val split = state.deadSplits.apply(0)
      state.deadSplits = state.deadSplits.drop(1)
      context.assignSplit(split, subtaskId)
    } else {
      val from = state.currentValue
      val until = state.currentValue + 1000
      context.assignSplit(new IntRangeSplit(from, until, from), subtaskId)
      state.currentValue = until
    }
  }

  override def addSplitsBack(splits: util.List[IntRangeSplit], subtaskId: Int): Unit = {
    if (splits.size() > 0) {
      val s = splits.asScala.toList
      state.deadSplits = s ::: state.deadSplits
    }
  }

  override def addReader(subtaskId: Int): Unit = {}

  override def snapshotState(checkpointId: Long): EnumeratorState = state

  override def close(): Unit = ???
}
