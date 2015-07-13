package ian.pdd

import org.apache.spark.Partitioner

/**
 * @author ian
 */
class BalancedPartitioner(parts: Int) extends Partitioner {

  /**
   * @author ian
   */

  // parts the total number of partitions
  // eles the total number of elements

  def getPartition(key: Any): Int = {
    val k = key.asInstanceOf[Long].intValue()
    // `k` is assumed to go continuously from 0 to elements-1.
    return k
  }

  def numPartitions(): Int = {
    return parts
  }
}

