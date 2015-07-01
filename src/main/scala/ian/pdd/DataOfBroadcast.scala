package ian.pdd

import org.apache.spark.rdd._
import ian.ISAXIndex._
import org.junit.Assert._
import org.apache.spark.broadcast._

/**
 * @author ian
 */
class DataOfBroadcast(_data: Broadcast[Array[Double]] = null,
                      _winSize: Int = 100,
                      _mean: Double = 0.0,
                      _std: Double = 1.0) extends DataHandler {
  private val data = _data
  def windowSize = _winSize
  private val mean = _mean
  private val std = _std

  def size(): Long = {
    data.value.size.toLong - windowSize + 1
  }

  def get(i: Long): Array[Double] = {
    assertTrue("ID " + i + " is out of range!", i >= 0 && i < size());
    val seq = data.value.slice(i.toInt, i.toInt + windowSize)
    return TSUtils.zNormalize(seq, mean, std)
  }

  def getViaIterator(seq: Array[Double]): Array[Double] = {
    return TSUtils.zNormalize(seq, mean, std)
  }

}