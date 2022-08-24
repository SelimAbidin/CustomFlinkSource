package org.example

import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}

class RichIntSource extends RichParallelSourceFunction[Long] {
  override def run(sourceContext: SourceFunction.SourceContext[Long]): Unit = {
    while (true) {
      val numberOfTasks = this.getRuntimeContext.getIndexOfThisSubtask
      (0 to numberOfTasks).foreach(_ => {
        sourceContext.collect(System.currentTimeMillis())
        Thread.sleep(10)
      })
      Thread.sleep(100)
    }
  }

  override def cancel(): Unit = ???
}
