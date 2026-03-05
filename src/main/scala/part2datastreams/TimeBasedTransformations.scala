package part2datastreams

import generators.shopping._
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, Watermark, WatermarkGenerator, WatermarkOutput, WatermarkStrategy}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessAllWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.{TumblingEventTimeWindows, TumblingProcessingTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

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


  class CountByWindowAll extends ProcessAllWindowFunction[ShoppingCartEvent, String, TimeWindow] {
    // this has context as first parameter instead of window which is more richer data structure which has more flink internal apis for access
    override def process(context: Context, elements: Iterable[ShoppingCartEvent], out: Collector[String]): Unit = {
      val window = context.window
      out.collect(s"Window [${window.getStart} - ${window.getEnd}] ${elements.size}")
    }
  }


  /*
   Group by window, every 3s, tumbling (non-overlapping), PROCESSING TIME
   */
    /*
  With processing time
  - we don't care when the event was created
  - multiple runs generate different results
   */
  def demoProcessingTime(): Unit = {
    def groupedEventsByWindow = shoppingCartEvents.windowAll(TumblingProcessingTimeWindows.of(Time.seconds(3)))
    def countEventsByWindow: DataStream[String] = groupedEventsByWindow.process(new CountByWindowAll)
    countEventsByWindow.print()
    env.execute()
  }


  /*
  With event time
  - we NEED to care about handling late data - done with watermarks
  - we don't care about Flink internal time
  - we might see faster results
  - same events + different runs => same results
   */
  def demoEventTime(): Unit = {
    // add watermark and timestamp for window for event time
    val groupedEventsByWindow = shoppingCartEvents
      .assignTimestampsAndWatermarks(
        WatermarkStrategy
          .forBoundedOutOfOrderness(java.time.Duration.ofMillis(500)) // max delay < 500 millis
          .withTimestampAssigner(new SerializableTimestampAssigner[ShoppingCartEvent] {
            override def extractTimestamp(element: ShoppingCartEvent, recordTimestamp: Long): Long = element.time.toEpochMilli
          })
      )
      .windowAll(TumblingEventTimeWindows.of(Time.seconds(3)))

    def countEventsByWindow: DataStream[String] = groupedEventsByWindow.process(new CountByWindowAll)
    countEventsByWindow.print()
    env.execute()
  }

  /**
   * Custom watermarks
   */
  // with every new MAX timestamp, every new incoming element with event time < max timestamp - max delay will be discarded
    class BoundedOutOfOrdernessGenerator(maxDelay: Long) extends WatermarkGenerator[ShoppingCartEvent] {
      var currentMaxTimestamp: Long = 0L // time starts at 0

      // maybe emit watermarks on particular event, every time we handle new event this on event method will get called, we get chance to emit watermark
      override def onEvent(event: ShoppingCartEvent, eventTimestamp: Long, output: WatermarkOutput): Unit = {
        //                 ^ event being processed   ^ timestamp of event  ^ mutable data structure to push watermark to for flink to handle later
        currentMaxTimestamp = Math.max(currentMaxTimestamp, event.time.toEpochMilli)
        // emitting a watermark is NOT manditory
        // output.emitWatermark(event.time.toEpochMilli) // every new event older than THIS EVENT will be discarded

      }

      // Flink can also call onPeriodicEmit to MAYBE emit watermarks regularly - up to us to maybe emit watermark at these times
      override def onPeriodicEmit(output: WatermarkOutput): Unit =
        output.emitWatermark(new Watermark(currentMaxTimestamp - maxDelay - 1))
  }

  // try to use our own custom watermark
  def demoEventTime_v2(): Unit = {

    // control how often Flink calls onPeriodicEmit
    env.getConfig.setAutoWatermarkInterval(1000L) // call onPeriodicEmit every 1 second


    // add watermark and timestamp for window for event time
    val groupedEventsByWindow = shoppingCartEvents
      .assignTimestampsAndWatermarks(
        WatermarkStrategy
          .forGenerator(_ => new BoundedOutOfOrdernessGenerator(500L)) // add max delay of 500 ms
          .withTimestampAssigner(new SerializableTimestampAssigner[ShoppingCartEvent] {
            override def extractTimestamp(element: ShoppingCartEvent, recordTimestamp: Long): Long = element.time.toEpochMilli
          })
      )
      .windowAll(TumblingEventTimeWindows.of(Time.seconds(3)))

    def countEventsByWindow: DataStream[String] = groupedEventsByWindow.process(new CountByWindowAll)
    countEventsByWindow.print()
    env.execute()
  }

  def main(args: Array[String]): Unit = {
    demoEventTime()
  }

}
