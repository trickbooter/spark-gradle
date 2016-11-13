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

package org.apache.spark

import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, Suite}

/** Shares a local `SparkContext` between all tests in a suite and closes it at the end */
trait SharedSparkSession extends BeforeAndAfterAll {
  self: Suite =>

  @transient private var _ss: SparkSession = _

  def ss: SparkSession = _ss

  var conf = new SparkConf(false)

  override def beforeAll() {
    super.beforeAll()
    _ss = SparkSession.builder()
      .appName("test")
      .master("local[2]")
      .config(conf)
      .getOrCreate()
  }

  override def afterAll() {
    try {
      LocalSparkSession.stop(_ss)
      _ss = null
    } finally {
      super.afterAll()
    }
  }
}