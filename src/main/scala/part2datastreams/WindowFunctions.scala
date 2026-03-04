package part2datastreams

import generators.gaming.{PlayerRegistered, ServerEvent, alice, bob, carl, mary, rob, sam}
import org.apache.flink.api.common.eventtime.TimestampAssignerSupplier.SupplierFromSerializableTimestampAssigner
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, TimestampAssigner, TimestampAssignerSupplier, WatermarkStrategy}
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.{AllWindowFunction, ProcessAllWindowFunction}
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import part2datastreams.WindowFunctions.serverStartTime

import java.time.Instant
import scala.concurrent.duration.DurationInt

object WindowFunctions {
  // use-case: steam of events for a gaming session

  val env = StreamExecutionEnvironment.getExecutionEnvironment
  implicit val serverStartTime: Instant = Instant.parse("2022-02-02T00:00:00.000Z")
  val events: List[ServerEvent] = List(
    bob.register(2.seconds), // player "Bob" registered 2s after server started
    bob.online(2.seconds),
    sam.register(3.seconds),
    sam.online(4.seconds),
    rob.register(4.seconds),
    alice.register(4.seconds),
    mary.register(6.seconds),
    mary.online(6.seconds),
    carl.register(8.seconds),
    rob.online(10.seconds),
    alice.online(10.seconds),
    carl.online(10.seconds)
  )

  val eventStream: DataStream[ServerEvent] = env
    .fromCollection(events)
    .assignTimestampsAndWatermarks( // extract timestamps for events (event time) + watermarks
      WatermarkStrategy
        .forBoundedOutOfOrderness(java.time.Duration.ofMillis(500)) // once you get an event with time T, you will not accept further events with time T - 500
        .withTimestampAssigner(new SerializableTimestampAssigner[ServerEvent] {
          override def extractTimestamp(element: ServerEvent, recordTimestamp: Long): Long =
            element.eventTime.toEpochMilli
        })
    )

  // how many players were registered every 3 seconds? (window function)
  // [0...3s] [3s..6s] [6s...9s]
  val threeSecondsTumblingWindow = eventStream.windowAll(TumblingEventTimeWindows.of(Time.seconds(3)))


  // count by windowAll
  class CountByWindowAll extends AllWindowFunction[ServerEvent, String, TimeWindow] {
    //                                             ^ input      ^ output  ^ window type
    override def apply(window: TimeWindow, input: Iterable[ServerEvent], out: Collector[String]): Unit = {
      val registrationEventCount = input.count(event => event.isInstanceOf[PlayerRegistered])
      out.collect(s"Window [${window.getStart} - ${window.getEnd}] $registrationEventCount")
    }
  }

  def demoCountByWindow(): Unit = {
    val registrationsPerThreeSeconds: DataStream[String] = threeSecondsTumblingWindow.apply(new CountByWindowAll)
    registrationsPerThreeSeconds.print()
    env.execute()
  }

  // alternative: process window function which offers a much richer API (lower level)
  class CountByWindowAllV2 extends ProcessAllWindowFunction[ServerEvent, String, TimeWindow] {
    // this has context as first parameter instead of window which is more richer data structure which has more flink internal apis for access
    override def process(context: Context, elements: Iterable[ServerEvent], out: Collector[String]): Unit = {
      val window = context.window
      val registrationEventCount = elements.count(event => event.isInstanceOf[PlayerRegistered])
      out.collect(s"Window [${window.getStart} - ${window.getEnd}] $registrationEventCount")

    }
  }

  def demoCountbyWindow_v2(): Unit = {
    val registrationsPerThreeSeconds: DataStream[String] = threeSecondsTumblingWindow.process(new CountByWindowAllV2)
    registrationsPerThreeSeconds.print()
    env.execute()
  }

  // alternative 2: aggregate function
  class CountByWindowV3 extends AggregateFunction[ServerEvent, Long, Long] {
    //                                              ^ input      ^ acc ^ output

    // start counting from 0
    override def createAccumulator(): Long = 0L

    // every element increases accumulator by 1
    override def add(value: ServerEvent, accumulator: Long): Long =
      if (value.isInstanceOf[PlayerRegistered]) accumulator + 1
      else accumulator

    // push a final output out of the final accumulator
    override def getResult(accumulator: Long): Long = accumulator

    // take two accumulators and take a bigger accumulator
    override def merge(a: Long, b: Long): Long = a + b
  }

  def demoCountbyWindow_v3(): Unit = {
    val registrationsPerThreeSeconds: DataStream[Long] = threeSecondsTumblingWindow.aggregate(new CountByWindowV3)
    registrationsPerThreeSeconds.print()
    env.execute()
  }



  def main(args: Array[String]): Unit = {
    demoCountbyWindow_v3()
  }
}
