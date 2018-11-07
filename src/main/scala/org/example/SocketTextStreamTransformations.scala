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

/**
 * This example shows an implementation of WordCount with data from a text socket.
 * To run the example make sure that the service providing the text data is already up and running.
 *
 * To start an example socket text stream on your local machine run netcat from a command line, 
 * where the parameter specifies the port number:
 *
 * {{{
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
object SocketTextStreamTransformations {

  def main(args: Array[String]) {
    if (args.length != 2) {
      System.err.println("USAGE:\nSocketTextStreamTransformations <hostname> <port>")
      return
    }

    val hostName = args(0)
    val port = args(1).toInt

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val text = env.socketTextStream(hostName, port)

    // Demonstrate basic transformations like map, filter, and union
    val mapped = text.map("mapped: " + _)
    val filtered = text.filter(_.head.isLower).map("filter lower: " + _)

    val splitted = text.split(inputStr => if(inputStr.head.isLower) List("lower") else List("upper"))
    val splitLower = splitted.select("lower").map("split lower: " + _)
    val splitUpper = splitted.select("upper").map("split upper: " + _)

    val union = mapped
      .union(filtered)
      .union(splitLower)
      .union(splitUpper)

    union print

    env.execute("Scala SocketTextStreamWordCount Example")
  }

}
