package ian.pdd

import org.apache.spark.Partitioner

/**
 * @author ian
 */
class ExactPartitioner(parts: Int, eles: Long) extends Partitioner {
  
  private val elesPerPart = Math.ceil(eles.toDouble/parts.toDouble).toLong
  
  // parts the total number of partitions
  // eles the total number of elements
  
  def getPartition(key: Any): Int = {
    val k = key.asInstanceOf[Long]
    // `k` is assumed to go continuously from 0 to elements-1.
    return (k / elesPerPart).toInt
  }

  def numPartitions(): Int = {
    return parts
  }
}
