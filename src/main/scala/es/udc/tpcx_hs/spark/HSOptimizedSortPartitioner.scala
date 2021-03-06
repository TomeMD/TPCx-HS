package es.udc.tpcx_hs.spark

import com.google.common.primitives.Longs
import org.apache.spark.Partitioner

case class HSOptimizedSortPartitioner(numPartitions: Int) extends Partitioner {

  import HSOptimizedSortPartitioner._

  val rangePerPart = (max - min) / numPartitions

  override def getPartition(key: Any): Int = {
    val b = key.asInstanceOf[Array[Byte]]
    val prefix = Longs.fromBytes(0, b(0), b(1), b(2), b(3), b(4), b(5), b(6))
    (prefix / rangePerPart).toInt
  }
}

object HSOptimizedSortPartitioner {
  val min = Longs.fromBytes(0, 0, 0, 0, 0, 0, 0, 0)
  val max = Longs.fromBytes(0, -1, -1, -1, -1, -1, -1, -1)  // 0xff = -1
}
