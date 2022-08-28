package org.example.custom.source

import org.apache.flink.api.connector.source.SourceSplit

class IntRangeSplit(val from: Int,val  until: Int, var currentValue:Int) extends SourceSplit with Serializable {
  override def splitId(): String = s"${from}_${until}"
}
