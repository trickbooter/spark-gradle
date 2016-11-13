package com.trickbooter

import org.apache.spark.ProvidedLocalSparkContext
import com.trickbooter.SparkPi.computePi

/**
  * Created by paul on 13/11/16.
  */
class SparkPiSpec extends UnitSpec with ProvidedLocalSparkContext {

  "computePi" should "compute pi correctly to 2dp" in {
    assert(~=(computePi(sc, 100), 3.14d, 0.005))
  }

  def ~=(x: Double, y: Double, precision: Double) = {
    if ((x - y).abs < precision) true else false
  }

}


