package part2datastreams

import generators.gaming.{PlayerRegistered, ShoppingCartEvent, alice, bob, carl, mary, rob, sam}
import org.apache.flink.api.common.eventtime.TimestampAssignerSupplier.SupplierFromSerializableTimestampAssigner
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, TimestampAssigner, TimestampAssignerSupplier, WatermarkStrategy}
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.{AllWindowFunction, ProcessAllWindowFunction, ProcessWindowFunction, WindowFunction}
import org.apache.flink.streaming.api.windowing.assigners.{EventTimeSessionWindows, GlobalWindows, SlidingEventTimeWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger
import org.apache.flink.streaming.api.windowing.windows.{GlobalWindow, TimeWindow}
import org.apache.flink.util.Collector
import part2datastreams.WindowFunctions.serverStartTime

import java.time.Instant
import scala.concurrent.duration.DurationInt

object WindowFunctions {
  // use-case: steam of events for a gaming session

  val env = StreamExecutionEnvironment.getExecutionEnvironment
  implicit val serverStartTime: Instant = Instant.parse("2022-02-02T00:00:00.000Z")
  val events: List[ShoppingCartEvent] = List(
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

  val eventStream: DataStream[ShoppingCartEvent] = env
    .fromCollection(events)
    .assignTimestampsAndWatermarks( // extract timestamps for events (event time) + watermarks
      WatermarkStrategy
        .forBoundedOutOfOrderness(java.time.Duration.ofMillis(500)) // once you get an event with time T, you will not accept further events with time T - 500
        .withTimestampAssigner(new SerializableTimestampAssigner[ShoppingCartEvent] {
          override def extractTimestamp(element: ShoppingCartEvent, recordTimestamp: Long): Long =
            element.eventTime.toEpochMilli
        })
    )

  // how many players were registered every 3 seconds? (window function)
  // [0...3s] [3s..6s] [6s...9s]
  val threeSecondsTumblingWindow = eventStream.windowAll(TumblingEventTimeWindows.of(Time.seconds(3)))


  // count by windowAll
  class CountByWindowAll extends AllWindowFunction[ShoppingCartEvent, String, TimeWindow] {
    //                                             ^ input      ^ output  ^ window type
    override def apply(window: TimeWindow, input: Iterable[ShoppingCartEvent], out: Collector[String]): Unit = {
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
  class CountByWindowAllV2 extends ProcessAllWindowFunction[ShoppingCartEvent, String, TimeWindow] {
    // this has context as first parameter instead of window which is more richer data structure which has more flink internal apis for access
    override def process(context: Context, elements: Iterable[ShoppingCartEvent], out: Collector[String]): Unit = {
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
  class CountByWindowV3 extends AggregateFunction[ShoppingCartEvent, Long, Long] {
    //                                              ^ input      ^ acc ^ output

    // start counting from 0
    override def createAccumulator(): Long = 0L

    // every element increases accumulator by 1
    override def add(value: ShoppingCartEvent, accumulator: Long): Long =
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

  /**
   * keyed stream and window functions
   *
   */
    // each element will be assigned to a 'mini-stream' for its own key
    // one task processes all the data for a particular key
    val streamByType: KeyedStream[ShoppingCartEvent, String] = eventStream.keyBy(e => e.getClass.getSimpleName)

    // for every key, we will a separate window for it
    val threeSecondsTumblingWindowsByType = streamByType.window(TumblingEventTimeWindows.of(Time.seconds(3)))

    class CountByWindow extends WindowFunction[ShoppingCartEvent, String, String, TimeWindow] {
      //                                       ^ input     ^ key    ^ output  ^ window type

      override def apply(key: String, window: TimeWindow, input: Iterable[ShoppingCartEvent], out: Collector[String]): Unit =
        out.collect(s"$key: $window, ${input.size}")
    }

    def demoCountByTypeByWindow(): Unit = {
      val finalStream = threeSecondsTumblingWindowsByType.apply(new CountByWindow)
      finalStream.print()
      env.execute()
    }

    // alternative - process function for windows with key by
    class CountByWindowV2 extends ProcessWindowFunction[ShoppingCartEvent, String, String, TimeWindow] {
      //                                                ^ input       ^ key ^ output.^ window type
      // context gives access to extra flink apis
      override def process(key: String, context: Context, elements: Iterable[ShoppingCartEvent], out: Collector[String]): Unit =
        out.collect(s"$key: ${context.window}, ${elements.size}")
    }

    def demoCountByTypeByWindow_v2(): Unit = {
      val finalStream = threeSecondsTumblingWindowsByType.process(new CountByWindowV2)
      finalStream.print()
      env.execute()
    }

    /**
     * Sliding Windows
     */

    // how many players were registered every 3 seconds, Updated Every 1s?
    // [0s...3s] [1s...4s] [2s...5s] [3s...6s] [4s...7s] [5s...8s] [6s...9s]
    def demoSlidingAllWindows(): Unit = {
      val windowSize: Time = Time.seconds(3)
      val slidingTime: Time = Time.seconds(1)
      val slidingWindowAll = eventStream.windowAll(SlidingEventTimeWindows.of(windowSize, slidingTime))

      // process the windowed stream with similar window functions
      val registrationCountByWindow = slidingWindowAll.apply(new CountByWindowAll)

      // similar to the other example
      registrationCountByWindow.print()
      env.execute()
    }

  /**
   * Session Windows - group of events with no more that a certain time gap in between them
   */
    // how many registration events do we have no more than 1 second apart

    def demoSessionWindows(): Unit = {
      val groupBySessionWindows = eventStream.windowAll(EventTimeSessionWindows.withGap(Time.seconds(1)))

      // operate any kind of window function
      val countBySession = groupBySessionWindows.apply(new CountByWindowAll)

      // same as things before
      countBySession.print()
      env.execute()
    }

  /** Global window functions */
    // how many registration events do we have every 10 events (not concerning time)

  // count by windowAll
  class CountByGlobalWindowAll extends AllWindowFunction[ShoppingCartEvent, String, GlobalWindow] {
    //                                             ^ input      ^ output  ^ window type
    override def apply(window: GlobalWindow, input: Iterable[ShoppingCartEvent], out: Collector[String]): Unit = {
      val registrationEventCount = input.count(event => event.isInstanceOf[PlayerRegistered])
      out.collect(s"Window [${window}] $registrationEventCount")
    }
  }


  def demoGlobalWindow(): Unit = {
      val globalWindowEvents = eventStream.windowAll(GlobalWindows.create())
        .trigger(CountTrigger.of[GlobalWindow](10))
        .apply(new CountByGlobalWindowAll) // not compatible with countbywindow all since window type is different now
      globalWindowEvents.print()
    env.execute()
  }

  /**
   * Exercise - what was the time window (continout 2s) when we had THE MOST registration events?
   *
   * */

    class CountRegistrationsByWindow() extends AllWindowFunction[ShoppingCartEvent, (TimeWindow, Int), TimeWindow] {

      override def apply(window: TimeWindow, input: Iterable[ShoppingCartEvent], out: Collector[(TimeWindow, Int)]): Unit = {
        val registrationEventCount = input.count(event => event.isInstanceOf[PlayerRegistered])
        out.collect(window, registrationEventCount)
      }
    }

    def exercise(): Unit = {
      val windowSize: Time = Time.seconds(2)
      val slidingTime: Time = Time.seconds(1)
      val slidingWindowAll = eventStream.windowAll(SlidingEventTimeWindows.of(windowSize, slidingTime))
      val resultStream = slidingWindowAll.apply(new CountRegistrationsByWindow)

      val resultCollection = resultStream.executeAndCollect()

      val max: (TimeWindow, Int) = resultCollection.max((a: (TimeWindow, Int) ,b: (TimeWindow, Int)) => a._2.compare(b._2))

      println(max)

      env.execute()
    }

  def main(args: Array[String]): Unit = {
    exercise()
  }
}
