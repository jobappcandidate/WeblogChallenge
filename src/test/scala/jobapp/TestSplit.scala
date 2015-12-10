package jobapp

import org.scalatest.FunSuite

/**
 * Created by khachik on 2015-12-10.
 */
class TestSplit extends FunSuite {


  test("Simple case") {
    val x = List(1, 3, 2, 4, 5).span(x => x < 3)
    val values = Array[Long](5, 3, 1, 17, 15, 13, 21)
    val result = Weblog.split[Long](values, x => x, 2)
    assertResult(3)(result.size)
    assertResult(Seq[Long](1, 3, 5))(result(0))
    assertResult(Seq[Long](13, 15, 17))(result(1))
    assertResult(Seq[Long](21))(result(2))
  }
}
