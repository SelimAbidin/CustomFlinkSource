package org.example.custom.source

import org.apache.flink.api.connector.source.{ReaderOutput, SourceReader, SourceReaderContext}
import org.apache.flink.core.io.InputStatus

import java.util
import java.util.concurrent.CompletableFuture
import scala.collection.JavaConverters._

class IntRangeReader(context:SourceReaderContext) extends SourceReader[Int, IntRangeSplit]{
  private var currentSplit: IntRangeSplit = null;
  private var availability: CompletableFuture[Void] = CompletableFuture.completedFuture(null)
  private var currentValue = 0
  override def start(): Unit = {}

  override def pollNext(output: ReaderOutput[Int]): InputStatus = {

    if (currentSplit != null && currentValue < currentSplit.until) {
      output.collect(currentValue)
      Thread.sleep(100)
      currentValue+=1
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
    currentSplit = splits.get(0)
    currentValue = currentSplit.from
    availability.complete(null)
  }

  override def notifyNoMoreSplits(): Unit = {
    
  }

  override def close(): Unit = ???
}