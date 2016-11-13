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

import _root_.io.netty.util.internal.logging.{InternalLoggerFactory, Slf4JLoggerFactory}
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.BeforeAndAfterEach
import org.scalatest.Suite

/** Manages a local `sc` {@link SparkContext} variable, correctly stopping it after each test. */
trait LocalSparkSession extends BeforeAndAfterEach with BeforeAndAfterAll {
  self: Suite =>

  @transient private var _ss: SparkSession = _
  @transient var ss: SparkSession = _

  override def beforeAll() {
    super.beforeAll()
    InternalLoggerFactory.setDefaultFactory(new Slf4JLoggerFactory())
  }

  override def afterEach() {
    try {
      resetSparkContext()
    } finally {
      super.afterEach()
    }
  }

  def resetSparkContext(): Unit = {
    LocalSparkSession.stop(ss)
    ss = null
  }

}

object LocalSparkSession {
  def stop(ss: SparkSession) {
    if (ss != null) {
      ss.stop()
    }
    // To avoid RPC rebinding to the same port, since it doesn't unbind immediately on shutdown
    System.clearProperty("spark.driver.port")
  }

  /** Runs `f` by passing in `ss` and ensures that `ss` is stopped. */
  def withSpark[T](ss: SparkSession)(f: SparkSession => T): T = {
    try {
      f(ss)
    } finally {
      stop(ss)
    }
  }
}

/** Manages a local `ss` {@link SparkSession} variable, creating it before each test and correctly stopping it after each test. */
trait ProvidedLocalSparkSession extends LocalSparkSession {
  self: Suite =>

  private val master = "local[2]"
  private val appName = "test"

  override def beforeEach() {
    ss = SparkSession.builder()
      .appName(appName)
      .master(master)
      .getOrCreate()
  }
}