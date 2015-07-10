package ian.pdd

import org.apache.spark.rdd._
import ian.ISAXIndex._
import org.apache.spark.broadcast._

/**
 * @author ian
 */
class DataOfBroadcast(_data: Broadcast[Array[Double]] = null,
                      _winSize: Int = 100) extends DataHandler {
  private val data = _data
  def windowSize = _winSize

  def size(): Long = {
    data.value.size.toLong - windowSize + 1
  }

  def get(i: Long): Array[Double] = {
    assert(i >= 0 && i < size(), "ID " + i + " is out of range!");
    val seq = data.value.slice(i.toInt, i.toInt + windowSize)
    return TSUtils.zNormalize(seq)
  }

  def getViaIterator(seq: Array[Double]): Array[Double] = {
    return TSUtils.zNormalize(seq)
  }

}