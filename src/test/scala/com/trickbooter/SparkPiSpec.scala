package com.trickbooter

import org.apache.spark.ProvidedLocalSparkSession
import com.trickbooter.SparkPi.computePi

/**
  * Created by paul on 13/11/16.
  */
class SparkPiSpec extends UnitSpec with ProvidedLocalSparkSession {

  "computePi" should "compute pi correctly to 2dp" in {
    assert(~=(computePi(ss, 100), 3.14d, 0.005))
  }

  def ~=(x: Double, y: Double, precision: Double) = {
    if ((x - y).abs < precision) true else false
  }

}


