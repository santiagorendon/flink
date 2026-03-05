package part2datastreams

import generators.shopping._
import org.apache.flink.api.common.functions.Partitioner
import org.apache.flink.streaming.api.scala._

object Partitions {

  // splitting = partitioning

  def demoPartitioner(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val shoppingCartEvents: DataStream[ShoppingCartEvent] =
      env.addSource(new SingleShoppingCartEventsGenerator(100)) // ~10 events/s

    // logic to split the data
    val partitioner = new Partitioner[String] {

      override def partition(key: String, numPartitions: Int): Int = { // invoked on every event to find the partition
        // has code % number of partitions
        println(s"Number of max partitions: ${numPartitions}")
        key.hashCode % numPartitions
      }
    }

    val partitionedStream = shoppingCartEvents.partitionCustom(
      partitioner,
      event => event.userId
    )

    /*
    bad because
    - you lose parallelism
    - you risk overloading the task with the disproportionate data

    Good for when a single task should interact with outside world, e.g sending HTTP requests
     */
    val badPartitioner = new Partitioner[String] {
      override def partition(key: String, numPartitions: Int): Int = { // invoked on every event to find the partition
        numPartitions - 1 // last partitionIndex
      }
    }

    val badPartitionedStream = shoppingCartEvents.partitionCustom(
      badPartitioner,
      event => event.userId
    )
    // redistribution of data evenly so will fix the bad partitioner
    // can lead to performance degradation because it involves DATA TRANSFER through the NETWORK
      .shuffle


    badPartitionedStream.print()
    env.execute()


  }
  def main(args: Array[String]): Unit = {
    demoPartitioner()
  }
}
