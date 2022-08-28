package org.example.custom.source

import org.apache.flink.api.connector.source.{Boundedness, Source, SourceReader, SourceReaderContext, SplitEnumerator, SplitEnumeratorContext}
import org.apache.flink.core.io.SimpleVersionedSerializer

class IntSource extends Source[Int, IntRangeSplit, EnumeratorState] {

  override def getBoundedness: Boundedness = Boundedness.CONTINUOUS_UNBOUNDED

  override def createReader(readerContext: SourceReaderContext): SourceReader[Int, IntRangeSplit] = new IntRangeReader(readerContext)

  override def createEnumerator(enumContext: SplitEnumeratorContext[IntRangeSplit]): SplitEnumerator[IntRangeSplit, EnumeratorState] = new IntEnumerator(enumContext)

  // Enumerator is initialized with previous enumerator state.
  override def restoreEnumerator(enumContext: SplitEnumeratorContext[IntRangeSplit], checkpoint: EnumeratorState): SplitEnumerator[IntRangeSplit, EnumeratorState] = new IntEnumerator(enumContext, checkpoint)

  override def getSplitSerializer: SimpleVersionedSerializer[IntRangeSplit] = new SimpleSerializer[IntRangeSplit]

  override def getEnumeratorCheckpointSerializer: SimpleVersionedSerializer[EnumeratorState] = new SimpleSerializer[EnumeratorState]
}
