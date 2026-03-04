package part2datastreams

import generators.shopping._
import org.apache.flink.streaming.api.scala._

import java.time.Instant

object TimeBasedTransformations {

  val env = StreamExecutionEnvironment.getExecutionEnvironment

  val shoppingCartEvents = env.addSource(new ShoppingCartEventsGenerator(
    sleepMillisPerEvent = 100, // time between 2 new events on average
    batchSize = 5, // generate events in groups of 5
    baseInstant = Instant.parse("2022-02-15T00:00:00.000Z" // start of application
    )
  ))

  // 1. Event time = the moment the event was CREATED
  // 2. Processing time = the moment the event ARRIVES AT FLINK



  /*
   Group by window, every 3s, tumbling (non-overlapping), PROCESSING TIME
   */
  def demoProcessingTime(): Unit = {
    def groupedEventsByWindow = shoppingCartEvents.windowAll()
  }

  def main(args: Array[String]): Unit = {

  }

}
