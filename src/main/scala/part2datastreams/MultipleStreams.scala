package part2datastreams

import generators.shopping._
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

object MultipleStreams {

  /*
  - union
  - window join
  - interval join
  - connect
   */

  // Unioning = combine multiple stream outputs into just one
  def demoUnion(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // define two streams of the same type
    val shoppingCartEventsKafka: DataStream[ShoppingCartEvent] =
      env.addSource(new SingleShoppingCartEventsGenerator(300, sourceId = Option("kafka")))

    val shoppingCartEventsFiles: DataStream[ShoppingCartEvent] =
      env.addSource(new SingleShoppingCartEventsGenerator(1000, sourceId = Option("files")))

    // will contain combined output from kafka and files
    val combinedShoppingCartEventStream: DataStream[ShoppingCartEvent] =
      shoppingCartEventsKafka.union(shoppingCartEventsFiles)

    combinedShoppingCartEventStream.print()
    env.execute()
  }

  // window join = elements belong to the same window + some join condition
  def demoWindowJoins(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val shoppingCartEvents = env.addSource(new SingleShoppingCartEventsGenerator(1000, sourceId = Option("kafka")))

    val catalogEvents = env.addSource(new CatalogEventsGenerator(200))

    val joinedStream = shoppingCartEvents
      .join(catalogEvents)
      // provide join condition
      .where(shoppingCartEvent => shoppingCartEvent.userId)
      .equalTo(catalogEvent => catalogEvent.userId)
      // provide the same window grouping
      .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
      // do something with correlated events
      .apply {
        (shoppingCartEvent, catalogEvent) =>
          s"User ${shoppingCartEvent.userId} browsed at ${catalogEvent.time} and bought at ${shoppingCartEvent.time}"
      }

    joinedStream.print()
    env.execute()
  }

  // interval joins = correlation between events A and B if durationMin < timeA - timeB < durationMax
  // involves EVENT TIME

  def demoIntervalJoins(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val shoppingCartEvents =
      env.addSource(new SingleShoppingCartEventsGenerator(1000, sourceId = Option("kafka")))
        .assignTimestampsAndWatermarks(
          WatermarkStrategy.forBoundedOutOfOrderness(java.time.Duration.ofMillis(500))
            .withTimestampAssigner(new SerializableTimestampAssigner[ShoppingCartEvent] {
              override def extractTimestamp(element: ShoppingCartEvent, recordTimestamp: Long): Long =
                element.time.toEpochMilli
            })
        )

    val catalogEvents = env.addSource(new CatalogEventsGenerator(200))



  }




  def main(args: Array[String]): Unit = {
    demoWindowJoins()
  }

}
