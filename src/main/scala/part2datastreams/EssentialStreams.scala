package part2datastreams

import org.apache.flink.api.common.serialization.SimpleStringEncoder
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.scala._

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

  /**
   * Exercise: FizzBuzz on Flink
   * - take a stream of 100 natural numbers
   * - for every number
   *  - if n % 3 == 0 then return "fizz"
   *  - if n % 5 == 0 then return "buzz"
   *  - if both then return "fizzbuzz"
   * - print the numbers for which you said "fizzbuzz" to a file
   */

  def main(args: Array[String]): Unit = {
    // applicationTemplate()
    fizzBuzzExercise()
    println("Hello World")
  }

}
