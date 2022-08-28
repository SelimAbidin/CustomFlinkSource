package org.example.custom.source

import org.apache.flink.api.connector.source.{ReaderOutput, SourceReader, SourceReaderContext}
import org.apache.flink.core.io.InputStatus

import java.util
import java.util.concurrent.CompletableFuture
import scala.collection.JavaConverters._

class IntRangeReader(context: SourceReaderContext) extends SourceReader[Int, IntRangeSplit] {
  private var currentSplit: IntRangeSplit = null;
  private var availability: CompletableFuture[Void] = CompletableFuture.completedFuture(null)

  override def start(): Unit = {}

  override def pollNext(output: ReaderOutput[Int]): InputStatus = {

    if (currentSplit != null && currentSplit.currentValue < currentSplit.until) {
      output.collect(currentSplit.currentValue)
      Thread.sleep(100)
      currentSplit.currentValue += 1
      InputStatus.MORE_AVAILABLE
    } else {
      if (availability.isDone) {
        availability = new CompletableFuture()
        context.sendSplitRequest()
      }
      InputStatus.NOTHING_AVAILABLE
    }
  }


  override def snapshotState(checkpointId: Long): util.List[IntRangeSplit] = {
    List(currentSplit).asJava
  }

  override def isAvailable: CompletableFuture[Void] = availability

  override def addSplits(splits: util.List[IntRangeSplit]): Unit = {
    // One split is assigned per task in enumerator therefor we only use the first index.
    currentSplit = splits.get(0)
    // Data availability is over since we gat a split.
    availability.complete(null)
  }

  override def notifyNoMoreSplits(): Unit = {
    // Not implemented since we expect our is boundless.
  }

  override def close(): Unit = ???
}
