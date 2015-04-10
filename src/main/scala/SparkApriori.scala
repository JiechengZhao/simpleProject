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


import breeze.linalg.{Vector, DenseVector, squaredDistance}

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import scala.collection.immutable.HashSet
import org.apache.spark.rdd.RDD
/**
 * K-means clustering.
 *
 * This is an example implementation for learning how to use Spark. For more conventional use,
 * please refer to org.apache.spark.mllib.clustering.KMeans
 */
object SparkApriori {

   def showWarning() {
    System.err.println(
      """WARN: This is a naive implementation of Apriori and is given as an example!
      """.stripMargin)
  }

  def main(args: Array[String]) {

    if (args.length < 3) {
      System.err.println("Usage: SparkKMeans <file> <k> <support>")
      System.exit(1)
    }

    showWarning()

    val sparkConf = new SparkConf().setAppName("SparkApriori")
    val sc = new SparkContext(sparkConf)
    val lines = sc.textFile(args(0))
    val data = lines.map( _.split(',').map(_.toInt).toSet ).cache()
    val K = args(1).toInt
    val sup = (data.count * args(2).toDouble).toInt
    var keys  = data.flatMap( i => i ).distinct().map( (new HashSet()) + _ )
    val base = data.collect()
    println("the sup is "+sup)
    for ( ii <- 0 to K) {
      val rkeys = keys.filter( i => {
        base.count( i.subsetOf(_)) > sup
      })

      keys = rkeys.cartesian(rkeys).filter( i => {
        (i._1 & i._2).size == i._1.size - 1 
      }).map(i => i._1 | i._2).distinct()
    }
    val rkeys = keys.filter( i => {
      base.count( i.subsetOf(_)) > sup
    })
    val result = rkeys.collect()
    println("Final result:")
    result.foreach(println)
    sc.stop()
  }
}
