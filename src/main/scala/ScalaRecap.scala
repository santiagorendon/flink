import java.util.concurrent.Executors
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Success, Try, Failure}

object ScalaRecap {

  def main(args: Array[String]): Unit = {

    // value
    val aBoolean: Boolean = false
    var aVariable: Int = 56
    aVariable += 1

    // expressions
    val anIfExpression: String = if (2 > 3) "bigger" else "smaller"

    // instructions vs expressions
    val theUnit: Unit = println("Hello, Scala") // Unit === "void"

    // OOP
    class Animal
    class Cat extends Animal


    // similar to interface
    trait Carnivore {
      def eat(animal: Animal): Unit
    }

    // inheritance: extence <= 1 class, but inherit from >= 0 traits
    class Crocodile extends Animal with Carnivore {
      override def eat(animal: Animal): Unit = println("eating this poor fellow")
    }

    // singleton
    object MySingleton

    // companions
    object Carnivore

    // case classes
    case class Person(name: String, age: Int)

    // generics
    class MyList[A]

    // method notation
    // croc.eat(animal) OR croc eat animal
    val three = 1 + 2
    val three_v2 = 1.+(2)

    // functional programming
    val incrementer: Int => Int = x => x + 1
    val incremented = incrementer(4) // 5, same as incrementer.apply(4)

    // map flatMap filter = Higher order functions (HOFs)
    val processedList = List(1,2,3).map(incrementer) // [2,3,4]
    val aLongerList = List(1,2,3).flatMap(x => List(x, x + 1)) // [1,2 2,3 3,4]

    // for-comprehensions
    val checkerboard = List(1,2,3).flatMap(n => List('a', 'b', 'c').map(c => (n, c)))
    val checkerboard_v2 = for {
      n <- List(1,2,3)
      c <- List('a', 'b', 'c')
    } yield (n, c) // same

    // options and try
    val anOption: Option[Int] = Option(/** something that might be null **/43)
    val doubleOption = anOption.map(_ * 2)

    val anAttempt: Try[Int] = Try(12)
    val modifiedAttempt = anAttempt.map(_ * 10)

  // pattern matching
    val anUnknown: Any = 45
    val medal = anUnknown match {
      case 1 => "gold"
      case 2 => "silver"
      case 3 => "bronze"
      case _ => "no medal"
    }

    val optionDescription = anOption match {
      case Some(value) => s"the option is not empty $value"
      case None => "the option is empty"
    }

    // Futures
    implicit val ec: ExecutionContext = ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(8))
    val aFuture = Future(/* something to be evaluated on another thread*/ 1 + 999)

    // register callback when it finishes
    aFuture.onComplete {
      case Success(value) => println(s"the async meaning of lise is $value")
      case Failure(exception) => println(s"the meaning of value failed: $exception")
    }

    val aPartialFunction: PartialFunction[Try[Int], Unit] = {
      case Success(value) => println(s"the async meaning of lise is $value")
      case Failure(exception) => println(s"the meaning of value failed: $exception")
    }

    // map, flatMap, filter, ...
    val doubledAsyncMOL: Future[Int] = aFuture.map(_ * 2)

    // implicits
    // purposes for use case
    // 1 - implicit arguments and values (values that you do not need to think about
    // for example execution context we do not want to pass it around every future)
    implicit val timeout: Int = 3000
    def setTimeout(f: () => Unit)(implicit tout: Int) = {
      Thread.sleep(tout)
      f()
    }
    // notice how we do not pass in timeout int
    setTimeout(() => println("timeout"))

    // 2 - extension methods
    implicit class MyRichInt(number: Int) {
      def isEven: Boolean = number % 2 == 0
    }

    // can use implicit to ints
    val is2Even = 2.isEven // new RichInt(2).isEven

    // 3 - converions - discouraged and dagenrous to use
    implicit def String2Person(name: String): Person =
      new Person(name, 57)

    val daniel: Person = "Daniel" // runs string2Person("Daniel") due to implicit
  }

}
