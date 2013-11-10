package hanafey.maptest

import scala.language.higherKinds
import ichi.bench.Thyme.Benched
import ichi.bench._
import scala.util.Random

abstract class MapTest[A, B, C[_, _]] {
  def load(rn: IndexedSeq[A], n: Int): C[A, B]

  def read(rn: IndexedSeq[A], from: C[A, B], n: Int): Any
}

class Immut[A] extends MapTest[A, Int, Map] {
  def load(rn: IndexedSeq[A], n: Int): Map[A, Int] = {
    var cache = Map.empty[A, Int]
    var i = 0
    while (i < n) {
      cache = cache.updated(rn(i), i)
      i += 1
    }
    cache
  }

  def read(rn: IndexedSeq[A], from: Map[A, Int], n: Int): Unit = {
    var i = 0
    while (i < n) {
      val v = from(rn(i))
      if (v < i) throw new Exception(s"Failure $v, $i")
      i += 1
    }
  }
}

class Mut[A] extends MapTest[A, Int, collection.mutable.Map] {
  def load(rn: IndexedSeq[A], n: Int): collection.mutable.Map[A, Int] = {
    val cache = collection.mutable.Map.empty[A, Int]
    var i = 0
    while (i < n) {
      cache(rn(i)) = i
      i += 1
    }
    cache
  }

  def read(rn: IndexedSeq[A], from: collection.mutable.Map[A, Int], n: Int): Unit = {
    var i = 0
    while (i < n) {
      val v = from(rn(i))
      if (v < i) throw new Exception(s"Failure $v, $i")
      i += 1
    }
  }
}

class MutSized[A] extends Mut[A] {
  override def load(rn: IndexedSeq[A], n: Int): collection.mutable.Map[A, Int] = {
    val cache = collection.mutable.Map.empty[A, Int]
    cache.sizeHint(n)
    var i = 0
    while (i < n) {
      cache(rn(i)) = i
      i += 1
    }
    cache
  }

}

class JMut[A] extends MapTest[A, Int, java.util.HashMap] {
  def load(rn: IndexedSeq[A], n: Int): java.util.HashMap[A, Int] = {
    val cache = new java.util.HashMap[A, Int]
    var i = 0
    while (i < n) {
      cache.put(rn(i), i)
      i += 1
    }
    cache
  }

  def read(rn: IndexedSeq[A], from: java.util.HashMap[A, Int], n: Int): Unit = {
    var i = 0
    while (i < n) {
      val v = from.get(rn(i))
      if (v < i) throw new Exception(s"Failure $v, $i")
      i += 1
    }
  }
}

object ImmutInt extends Immut[Int]

object ImmutString extends Immut[String]

object MutInt extends Mut[Int]

object MutIntSized extends MutSized[Int]

object MutString extends Mut[String]

object JMutInt extends JMut[Int]

object JMutString extends JMut[String]


case class Result(title: String, count: Int, runtime: Double) extends Ordered[Result] {
  def compare(that: Result): Int = this.runtime.compareTo(that.runtime)
}

case class Operation(title: String, work: (Int) => () => Any)

class TestLoadFunction[A, B, C[_, _]](m: MapTest[A, B, C], keys: IndexedSeq[A]) extends Function[Int, () => Any] {
  def apply(n: Int): () => Any = {
    new (() => Any) {
      def apply() = m.load(keys, n)
    }
  }
}

class TestReadFunction[A, B, C[_, _]](m: MapTest[A, B, C], keys: IndexedSeq[A]) extends Function[Int, () => Any] {
  def apply(n: Int): () => Any = {
    val from = m.load(keys, n)
    new (() => Any) {
      def apply() = {
        m.read(keys, from, n)
      }
    }
  }
}

object RandomStrings {
  def nextString(n: Int, m: Int): String = {
    val sb = new StringBuilder(n)
    val length = Random.nextInt(n + 1 - m) + m
    for (i <- 1 to length) {
      sb += (Random.nextInt(24) + 65).toChar
    }
    sb.toString()
  }
}


object Main {
  val rn = (1 to 100000).map(i => Random.nextInt())
  val rs = (1 to 100000).map(i => RandomStrings.nextString(15, 6))

  def benchPress(thyme: Thyme, op: Operation, n: Int): Result = {
    val br = Benched.empty
    val work = op.work(n)
    thyme.benchWarm(thyme.Warm(work()))(br)
    Result(op.title, n, br.runtime * 1.0e9)
  }

  def niceTime(t: Double): String = {
    t match {
      case x if x < 100.0 => f"$x%.4f ns"
      case x if x < 100.0 * 1000.0 => val y = x / 1000.0; f"$y%.4f us"
      case x if x < 100.0 * 1000.0 * 1000.0 => val y = x / 1000.0 / 1000.0; f"$y%.4f ms"
      case x => val y = x / 1000.0 / 1000.0 / 1000.0; f"$y%.4f s"
    }
  }

  def report(title: String, results: List[List[Result]]) = {
    println()
    println(title)
    val ref :: rest = results
    println(("" :: ref.map(r => r.count)).mkString("\t"))
    println(
      List(
        ref.head.title,
        ref.map(r => niceTime(r.runtime)).mkString("\t")
      ).mkString("\t")
    )
    for (cr <- rest) {
      println(
        List(
          cr.head.title,
          cr.zip(ref).map(t => {
            val ratio = 100.0 * t._2.runtime / t._1.runtime
            f"$ratio%.2f"
          }).mkString("\t")
        ).mkString("\t")
      )
    }
  }

  def main(args: Array[String]) {

    val warm = if (args.length > 0) args(0).toUpperCase.startsWith("W") else false
    val its = if (args.length > 1) args(1).toInt else 1

    val th = if (warm) Thyme.warmed(verbose = print) else new Thyme
    val loadSize = List(25, 50, 100, 250, 500, 1000, 10000, 100000)

    val operations = List(
      ("Map Load, Int keys",
        List(
          Operation("J Hi", new TestLoadFunction(JMutInt, rn)),
          Operation("S Mi", new TestLoadFunction(MutInt, rn)),
          Operation("S Ii", new TestLoadFunction(ImmutInt, rn)))),
      ("Map Read, Int keys",
        List(
          Operation("J Hi", new TestReadFunction(JMutInt, rn)),
          Operation("S Mi", new TestReadFunction(MutInt, rn)),
          Operation("S Ii", new TestReadFunction(ImmutInt, rn))
        )
        ),
      ("Map Load, String keys",
        List(
          Operation("J Hs", new TestLoadFunction(JMutString, rs)),
          Operation("S Ms", new TestLoadFunction(MutString, rs)),
          Operation("S Is", new TestLoadFunction(ImmutString, rs)))),
      ("Map Read, String keys",
        List(
          Operation("J Hs", new TestReadFunction(JMutString, rs)),
          Operation("S Ms", new TestReadFunction(MutString, rs)),
          Operation("S Is", new TestReadFunction(ImmutString, rs))
        )
        )
    )

    val tailFraction = 0.25
    val tailN = Math.round(its * tailFraction) match {
      case n if n < 1 => if (its > 2) 1 else 0
      case n if n * 2 >= its => if (its % 2 == 0) its / 2 - 2 else its / 2 // Median
      case n => n.toInt
    }

    for ((heading, ops) <- operations) {
      val results = for (op <- ops) yield {
        for (n <- loadSize) yield {
          val r = for (it <- 1 to its) yield {
            benchPress(th, op, n)
          }
          val rs = r.sorted
          val robustRuntime = r.slice(tailN, its - tailN).foldLeft(0.0)((z, e) => z + e.runtime) / (its - 2 * tailN)
          rs.head.copy(runtime = robustRuntime)
        }
      }
      report(heading + " (Relative to Java HashMap", results)
      report(heading + " (Relative to Scala Mutable Map", results.tail)
    }

  }
}
