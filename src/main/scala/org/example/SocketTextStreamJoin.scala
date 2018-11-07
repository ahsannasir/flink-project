package org.example

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

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * This example shows an implementation of a stream join with data from a text socket.
  * To run the example make sure that the service providing the text data is already up and running.
  *
  * To start an example socket text stream on your local machine run netcat from a command line,
  * where the parameter specifies the port number:
  *
  * {{{
  *   nc -lk 9998
  *   nc -lk 9999
  * }}}
  *
  * Usage:
  * {{{
  *   SocketTextStreamWordCount <hostname> <port> <output path>
  * }}}
  *
  * This example shows how to:
  *
  *   - use StreamExecutionEnvironment.socketTextStream
  *   - write a simple Flink Streaming program in scala.
  *   - write and use user-defined functions.
  */
object SocketTextStreamJoin {

  def main(args: Array[String]) {
    if (args.length != 3) {
      System.err.println("USAGE:\nSocketTextStreamJoin <hostname> <port1> <port2>")
      return
    }

    val hostName = args(0)
    val port1 = args(1).toInt
    val port2 = args(2).toInt

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // Create two input streams
    val kv1 = env.socketTextStream(hostName, port1)
    val kv2 = env.socketTextStream(hostName, port2)

    case class kvTuple(key: String, value: String)

    // Split on whitespace, make tuple stream but catch empty and single strings
    def splitStream(stream: DataStream[String]): DataStream[kvTuple] = {
      stream.map( _.split("\\W+") match {
        case Array(kk, vv, _*) => kvTuple(kk, vv)
        case Array(kk) => kvTuple(kk, "")
        case _ => kvTuple("", "")
      })
    }

    val keyed1 = splitStream(kv1)
    val keyed2 = splitStream(kv2)

    // Create joined stream and output
    val joined = keyed1.join(keyed2)
        .where(_.key)
        .equalTo(_.key)
        .window(TumblingProcessingTimeWindows.of(Time.seconds(20)))
        .apply( (kv1, kv2) => (kv1.key, kv1.value, kv2.value) )

    joined print

    env.execute("Scala SocketTextStreamWordCount Example")
  }

}
