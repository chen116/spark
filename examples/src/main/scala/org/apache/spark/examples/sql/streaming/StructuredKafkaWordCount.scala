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

// scalastyle:off println
package org.apache.spark.examples.sql.streaming

import java.sql.Timestamp
import java.util.UUID

import com.qubole.sparklens.{QuboleJobListener, VicJobListener, VicSQLStream}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.window

/**
  * Consumes messages from one or more topics in Kafka and does wordcount.
  * Usage: StructuredKafkaWordCount <bootstrap-servers> <subscribe-type> <topics>
  *     [<checkpoint-location>]
  *   <bootstrap-servers> The Kafka "bootstrap.servers" configuration. A
  *   comma-separated list of host:port.
  *   <subscribe-type> There are three kinds of type, i.e. 'assign', 'subscribe',
  *   'subscribePattern'.
  *   |- <assign> Specific TopicPartitions to consume. Json string
  *   |  {"topicA":[0,1],"topicB":[2,4]}.
  *   |- <subscribe> The topic list to subscribe. A comma-separated list of
  *   |  topics.
  *   |- <subscribePattern> The pattern used to subscribe to topic(s).
  *   |  Java regex string.
  *   |- Only one of "assign, "subscribe" or "subscribePattern" options can be
  *   |  specified for Kafka source.
  *   <topics> Different value format depends on the value of 'subscribe-type'.
  *   <checkpoint-location> Directory in which to create checkpoints. If not
  *   provided, defaults to a randomized directory in /tmp.
  *
  * Example:
  *    `$ bin/run-example \
  *      sql.streaming.StructuredKafkaWordCount host1:port1,host2:port2 \
  *      subscribe topic1,topic2`
  */
object StructuredKafkaWordCount {
  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      System.err.println("Usage: StructuredKafkaWordCount <bootstrap-servers> " +
        "<subscribe-type> <topics> [<checkpoint-location>]")
      System.exit(1)
    }


    val Array(bootstrapServers, subscribeType, topics, _*) = Array(args(0),"subscribe","test")//args


    val checkpointLocation =
      if (args.length > 3) args(3) else "/tmp/temporary-" + UUID.randomUUID.toString

    val spark = SparkSession
      .builder
      //.master("local")
      .appName("meowStructuredKafkaWordCount")

      .getOrCreate()
    spark.sparkContext.setLogLevel("off")
//
//    val ql = new VicJobListener(spark.sparkContext.getConf,spark.sparkContext,spark.sparkContext.getExecutorIds())
//    //    val ql = new QuboleJobListener(spark.sparkContext.getConf)
//
//    val vs = new VicSQLStream(spark.sparkContext.getConf,ql)
//    spark.sparkContext.addSparkListener(ql)
//    spark.streams.addListener(vs)


    import spark.implicits._

    // Create DataSet representing the stream of input lines from kafka
    val lines = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrapServers)
      .option(subscribeType, topics)
      .load()


    val allFields = lines.selectExpr(
      s"CAST(value AS STRING)",

      s"timestamp as timestamp"

    ).as[(String,String)]


    val windowDuration = s"2 seconds"
    val slideDuration = s"1 seconds"


    // Split the lines into words, retaining timestamps
    val words = allFields.flatMap(line =>
      line._1.split(" ").map(word => (word, line._2))
    ).toDF("word", "timestamp")

    // Group the data by window and word and compute the count of each group
    val windowedCounts = words.groupBy(
      window($"timestamp", windowDuration, slideDuration), $"word"
    ).count().orderBy("window")

    // Start running the query that prints the windowed word counts to the console
    val query = windowedCounts.writeStream
      .outputMode("complete")
      .format("console")
      .option("truncate", "false")
      .start()









//    val myfunc = (word: String) => {
//
//      var a=0
//      var x: Double= 0
//      for( a <- 1 to 10){
//        x=Math.sqrt(   (a*(a+1)).toDouble)
//      }
//      println("   woof      meowwwwwwwwwwwwwwwwwwwwwwwww"+x)
//      (word)
//    }
//
//    // Generate running word count
//    //val wordCounts = lines.flatMap(_.split(" ")).groupBy("value").count()
//
//    val wordCounts = lines.map(words=> myfunc(words)).flatMap(line => line.split(" ")).groupBy("value").count()
//
//
//
//    // Start running the query that prints the running counts to the console
//    val query = wordCounts.writeStream
//      .outputMode("complete")
//      .format("console")
//      .option("checkpointLocation", checkpointLocation)
//      .start()

    query.awaitTermination()
  }

}
// scalastyle:on println
