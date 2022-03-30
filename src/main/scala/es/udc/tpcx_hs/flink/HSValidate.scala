/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package es.udc.tpcxhs

import com.google.common.primitives.UnsignedBytes
import org.apache.hadoop.util.PureJavaCrc32
import org.apache.flink.api.scala._
import org.apache.flink.hadoopcompatibility.scala.HadoopInputs

import scala.collection._
import scala.collection.mutable.ListBuffer

/**
 * An application that reads sorted data according to the terasort spec and
 * reports if it's indeed sorted.
 * This is an example program to validate TeraSort results
 *
 * Outputs data to HDFS if there is any error in validation
 *
 * See http://sortbenchmark.org/
 */

object HSValidate {

  def main(args: Array[String]) {

    if (args.length < 2) {
      println("Usage: flink run --class es.udc.tpcxhs.HSValidate TPCx-HS-master_Flink.jar <OUTPUT_SORT_PATH> <OUTPUT_VALIDATE_PATH>")
      return
    }

    // Parse arguments
    val inputFile = args(0)
    val outputFile = args(1)

    val env = ExecutionEnvironment.getExecutionEnvironment
    env.getConfig.enableObjectReuse()
    env.getConfig.enableForceKryo()
    env.setParallelism(1)

    val dataset = env.createInput(HadoopInputs.readHadoopFile(new HSInputFormat(), classOf[Array[Byte]], classOf[Array[Byte]], inputFile))
    validate(env, dataset, outputFile)

    env.execute("HSValidate")

  }

  def validate(env: ExecutionEnvironment, dataset: DataSet[(Array[Byte], Array[Byte])],
               outputFile: String): Unit = {
    val output: DataSet[(Unsigned16, Array[Byte], Array[Byte])] =
      dataset.mapPartition {
        iter => {

          val sum = new Unsigned16
          val checksum = new Unsigned16
          val crc32 = new PureJavaCrc32()
          val min = new Array[Byte](10)
          val max = new Array[Byte](10)

          val cmp = UnsignedBytes.lexicographicalComparator()

          var pos = 0L
          var prev = new Array[Byte](10)

          while (iter.hasNext) {
            val key = iter.next()._1
            assert(cmp.compare(key, prev) >= 0)

            crc32.reset()
            crc32.update(key, 0, key.length)
            checksum.set(crc32.getValue)
            sum.add(checksum)

            if (pos == 0) {
              key.copyToArray(min, 0, 10)
            }
            pos += 1
            prev = key
          }
          prev.copyToArray(max, 0, 10)
          Iterator((sum, min, max))
        }
      }

    val checksumOutput = output.collect()
    val cmp = UnsignedBytes.lexicographicalComparator()
    val sum = new Unsigned16

    checksumOutput.foreach {
      case (partSum, min, max) => sum.add(partSum)
    }
    println("checksum: " + sum.toString)
    val data = new ListBuffer[String]()
    data += sum.toString
    val error = new ListBuffer[String]()

    var lastMax = new Array[Byte](10)
    checksumOutput.map {
      case (partSum, min, max) => (partSum, min.clone(), max.clone())
    }.zipWithIndex.foreach {
      case ((partSum, min, max), i) =>
        println(s"part $i")
        println(s"lastMax" + lastMax.toSeq.map(x => if (x < 0) 256 + x else x))
        println(s"min " + min.toSeq.map(x => if (x < 0) 256 + x else x))
        println(s"max " + max.toSeq.map(x => if (x < 0) 256 + x else x))
        if (!(cmp.compare(min, max) <= 0)) {
          error += "min >= max"
      }
      if (!(cmp.compare(lastMax, min) <= 0)) {
        error += "current partition min < last partition max"
      }
      lastMax = max
    }

    println("checksum: " + sum.toString)
    println("partitions are properly sorted")
    if(error != null && error.size > 0 ) {
      env.fromCollection(error).writeAsText(outputFile)
    }else{
      env.fromCollection(data).writeAsText(outputFile)
    }

    if (error.size > 0) throw new Exception("Data Validation Failed using HSValidate")


  }
}
