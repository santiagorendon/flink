package part2datastreams

import org.apache.flink.api.common.functions.{FlatMapFunction, MapFunction, ReduceFunction}
import org.apache.flink.api.common.serialization.SimpleStringEncoder
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util

import org.apache.flink.util.Collector
import scala.collection.mutable.ArrayBuffer

object EssentialStreams {

  def applicationTemplate(): Unit = {
    // 1 - execution environment
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    // in between, add any sort of computations
    import org.apache.flink.streaming.api.scala._ // import TypeInformation for the data for your DataStreams (implicit generators for regular data types are generated immediately)
    val simpleNumberStream: DataStream[Int] = env.fromElements(1, 2, 3, 4)

    // perform some actions
    simpleNumberStream.print()


    // at the end
    env.execute() // will trigger all computations described earlier
  }

  // transformations
  def demoTransformations(): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val numbers: DataStream[Int] = env.fromElements(1, 2, 3, 4, 5)

    // checking parallelism
    println(s"Current parallelism ${env.getParallelism}")
    // set different parallelism
    env.setParallelism(2)
    println(s"New parallelism ${env.getParallelism}")

    // map
    val doubledNumbers: DataStream[Int] = numbers.map(_ * 2)

    // flatMap
    val expandedNumbers: DataStream[Int] = numbers.flatMap(n => List(n, n + 1))

    // filter
    val filteredNumbers: DataStream[Int] = numbers
      .filter(_ % 2 == 0)
    /* you can set parallelism here*/.setParallelism(4)

    val finalData = expandedNumbers.writeAsText("output/expandedStream.txt")
    finalData.setParallelism(3)

    env.execute()
  }

  /**
   * Exercise: FizzBuzz on Flink
   * - take a stream of 100 natural numbers
   * - for every number
   *  - if n % 3 == 0 then return "fizz"
   *  - if n % 5 == 0 then return "buzz"
   *  - if both then return "fizzbuzz"
   * - print the numbers for which you said "fizzbuzz" to a file
   */
  case class FizzBuzzResult(n: Int, output: String)

  def fizzBuzzExercise(): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val numbersArr: ArrayBuffer[Int] = ArrayBuffer()
    for (i <- 1 to 100) {
      numbersArr += i
    }
    println(numbersArr)

    // initiate array
    val numbers: DataStream[Int] = env.fromCollection(numbersArr)

    val resultStream: DataStream[Int] = numbers.map(n => {
      val output = if (n % 3 == 0 && n % 5 == 0) {
        "fizzbuzz"
      }
      else if (n % 3 == 0) {
        "fizz"
      }
      else if (n % 5 == 0) {
        "buzz"
      }
      else {
        n.toString()
      }
      FizzBuzzResult(n, output)
    }).filter(_.output == "fizzbuzz")
      .map(_.n)

    // alternative to
    // val finalData = resultStream.writeAsText("output/fizzBuzz.txt")
    // finalData.setParallelism(1)

    // add a Sink - datastructure describes how output data should be structured
    resultStream.addSink(
      StreamingFileSink.forRowFormat(
        new Path("output/streaming_sink"),
        new SimpleStringEncoder[Int]("UTF-8")
      )
        .build()
    ).setParallelism(1)

    env.execute()



  }

  // explicit transformations
  def demoExplicitTransformations(): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(8)
    val numbers = env.fromSequence(1, 100)

    // map
    val doubledNumbers = numbers.map(_ * 2)

    // explicit version
    val doubledNumbers_v2 = numbers.map(new MapFunction[Long, Long] {
      // declare fields, methods, ...
      override def map(value: Long) = value * 2
    })

    // flat map
    val expandedNumbers = numbers.flatMap(n => Range.Long(1, n, 1).toList)

    // explicit version
    val expandedNUmbers_v2 = numbers.flatMap(new FlatMapFunction[Long, Long] {
      override def flatMap(n: Long, out: Collector[Long]) =
        Range.Long(1, n, 1).foreach { i =>
          out.collect(i) //impreative style - pushed the new element downstream
        }
    })

    // process method
    // ProcessFunction is THE MOST GENERAL function to process elements in Flink
    val expandedNumbers_v3 = numbers.process(new ProcessFunction[Long, Long] {
      override def processElement(n: Long, ctx: ProcessFunction[Long, Long]#Context, out: util.Collector[Long]): Unit =
        Range.Long(1, n, 1).foreach { i =>
          out.collect(i) //impreative style - pushed the new element downstream
        }
    })

    // reduce
    val keyedNumbers: KeyedStream[Long, Boolean] = numbers.keyBy(n => n % 2 == 0)
    val sumByLKey = keyedNumbers.reduce(_ + _) // sum all elements by key

    // reduce - explicit apprhac
    val sumByKey_v2 = keyedNumbers.reduce(new ReduceFunction[Long] {
      // additonal fields, methods, etc.
      override def reduce(x: Long, y: Long): Long = x + y
    })

    sumByKey_v2.print()
    env.execute()

  }

  def main(args: Array[String]): Unit = {
    // applicationTemplate()
    // fizzBuzzExercise()
    demoExplicitTransformations()
    println("Hello World")
  }

}
