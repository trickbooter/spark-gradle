package com.trickbooter

import org.scalatest._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
abstract class UnitSpec extends FlatSpec with Matchers with
  OptionValues with Inside with Inspectors