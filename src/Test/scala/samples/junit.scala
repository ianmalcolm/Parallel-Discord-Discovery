package samples

import org.junit._
import Assert._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.mllib.rdd.RDDFunctions._
import ian.pdd.ExactPartitioner

@Test
class PDDTest {

  @Test
  def testOK() = assertTrue(true)

  @Test
  def slidingShouldYieldSameResultOrder() {

    val appName = "Parallel Discord Discovery"
    val master = "local[4]"
    val inputfile = "C:/users/ian/github/datasets/testdata1e6.txt"
    val windowSize = 10
    val partitionSize = 10
    val conf = new SparkConf().setAppName(appName).setMaster(master)
    val sc = new SparkContext(conf)

    val subsequences = sc.textFile(inputfile)
      .map(str => str.toDouble)
      .sliding(windowSize)
      .zipWithIndex()
      .map { case (k, v) => (v, k) }

    val sscol = subsequences.collect()

    val sscol2 = sc.textFile(inputfile)
      .sliding(windowSize)
      .collect()

    var i = 0
    for (i <- 0 until sscol2.size) { // sscol2.size
      if (sscol(i)._2(0) != sscol2(i)(0).toDouble) {
        assertTrue("No." + i.toInt + " mismatch", false)
      }
    }
  }

  @Test
  def exactpartitionerShouldReturnRightPartNum() {
    val parts = 10;
    val eles = 999991;
    val exactPart = new ExactPartitioner(parts, eles)
    val testSet = List[Long](0, 99999, 100000, 199999, 200000, 899999, 900000, 999991)
    val answerSet = List[Long](0, 0, 1, 1, 2, 8, 9, 9)
    var i = 0

    for (i <- 0 until testSet.size) {
      assertTrue("Given ExactPartitioner a key of " + testSet(i) + " should return part num " + answerSet(i) + ", but actually return part num " + exactPart.getPartition(testSet(i)), exactPart.getPartition(testSet(i)) == answerSet(i))
    }
  }

  //    @Test
  //    def testKO() = assertTrue(false)

}


