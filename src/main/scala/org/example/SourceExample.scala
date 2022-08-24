/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.example

import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.example.custom.source.IntSource


object SourceExample {

  def main(args: Array[String]) {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // checkpointing on every 5 minutes
    env.enableCheckpointing(1000 * 60 * 5)

    //  env.addSource(new RichIntSource())(TypeInformation.of(classOf[Long])).setParallelism(10).print().setParallelism(1)

    env.fromSource(new IntSource(), WatermarkStrategy.noWatermarks(), "")(TypeInformation.of(classOf[Int]))
      .setParallelism(3)
      .print()
      .setParallelism(1)

    env.execute("Flink Source Example")
  }
}
