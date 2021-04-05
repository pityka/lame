package lame
import org.scalatest.funsuite._
import org.scalatest.matchers.should._
import akka.stream.scaladsl._
import akka.util.ByteString
import scala.concurrent.Await
import scala.concurrent.duration._

class BlockGzipSuite extends AnyFunSuite with Matchers {
  def randomData(size: Int) = {
    val random = new scala.util.Random
    val buf = Array.fill[Byte](size)(0)
    random.nextBytes(buf)
    ByteString(buf)
  }

  test("correctness - 256") {
    implicit val AS = akka.actor.ActorSystem()

    val raw = randomData(256)
    val data2 = Await
      .result(
        Source
          .single(raw)
          .via(lame.BlockGzip())
          .map(_._1)
          .via(lame.BlockGunzip(emitFromOffsetOfFirstBlock = 0))
          .runWith(Sink.seq),
        Duration.Inf
      )
      .reduce(_ ++ _)

    raw shouldBe data2
    AS.terminate()
  }

  test("addressing - 256") {
    implicit val AS = akka.actor.ActorSystem()

    val raw = randomData(256)
    val data2 = Await
      .result(
        Source
          .single(raw)
          .via(lame.BlockGzip())
          .runWith(Sink.seq),
        Duration.Inf
      )
      .head

    data2._2 shouldBe List(0L)

    AS.terminate()
  }
  test("addressing - empty") {
    implicit val AS = akka.actor.ActorSystem()

    val data2 = Await
      .result(
        Source
          .apply(List())
          .via(lame.BlockGzip())
          .runWith(Sink.seq),
        Duration.Inf
      )

    data2.flatMap(_._2) shouldBe List()

    AS.terminate()
  }
  test("addressing - 256 * 3") {
    implicit val AS = akka.actor.ActorSystem()

    val raw = randomData(256)
    val data2 = Await
      .result(
        Source
          .apply(List(raw, raw, raw))
          .via(lame.BlockGzip())
          .runWith(Sink.seq),
        Duration.Inf
      )

    data2.flatMap(_._2) shouldBe List(0L, 256L, 512L)

    AS.terminate()
  }
  test("addressing - 90KB") {
    implicit val AS = akka.actor.ActorSystem()

    val raw = randomData(1024 * 90)
    val data2 = Await
      .result(
        Source
          .single(raw)
          .via(lame.BlockGzip())
          .runWith(Sink.seq),
        Duration.Inf
      )

    data2
      .flatMap(_._2)
      .map(vfp =>
        (BlockGunzip.getFileOffset(vfp), BlockGunzip.getBlockOffset(vfp))
      ) shouldBe Seq((0L, 0))

    AS.terminate()
  }
  test("addressing - 270KB") {
    implicit val AS = akka.actor.ActorSystem()

    val raw = randomData(1024 * 90)
    val data2 = Await
      .result(
        Source
          .apply(List(raw, raw, raw))
          .via(lame.BlockGzip())
          .runWith(Sink.seq),
        Duration.Inf
      )

    data2
      .flatMap(_._2)
      .map(vfp =>
        (BlockGunzip.getFileOffset(vfp), BlockGunzip.getBlockOffset(vfp))
      ) shouldBe Seq((0L, 0), (65536L, 26660), (131072L, 53320))

    val raw_blocks = 0 to 2 map { i =>
      Await
        .result(
          BlockGunzip
            .sourceFromFactory(data2.flatMap(_._2).apply(i))(fileOffSet =>
              Source
                .single(
                  data2.map(_._1).toList.reduce(_ ++ _).drop(fileOffSet.toInt)
                )
            )
            .runWith(Sink.seq),
          Duration.Inf
        )
        .reduce(_ ++ _)
        .take(raw.size)
    }
    raw_blocks shouldBe Seq(raw, raw, raw)

    AS.terminate()
  }
  test("addressing - 3x 48KB") {
    implicit val AS = akka.actor.ActorSystem()

    val raw1 = randomData(1024 * 48)
    val raw2 = randomData(1024 * 48)
    val raw3 = randomData(1024 * 48)
    val data2 = Await
      .result(
        Source
          .apply(List(raw1, raw2, raw3))
          .via(lame.BlockGzip())
          .runWith(Sink.seq),
        Duration.Inf
      )

    data2
      .flatMap(_._2)
      .map(vfp =>
        (BlockGunzip.getFileOffset(vfp), BlockGunzip.getBlockOffset(vfp))
      ) shouldBe Seq((0L, 0), (0L, 49152), (65536L, 32804))

    val raw_blocks = 0 to 2 map { i =>
      Await
        .result(
          BlockGunzip
            .sourceFromFactory(data2.flatMap(_._2).apply(i))(fileOffSet =>
              Source
                .single(
                  data2.map(_._1).toList.reduce(_ ++ _).drop(fileOffSet.toInt)
                )
            )
            .runWith(Sink.seq),
          Duration.Inf
        )
        .reduce(_ ++ _)
        .take(raw1.size)
    }
    raw_blocks shouldBe Seq(raw1, raw2, raw3)

    AS.terminate()
  }
  test("correctness - 0") {
    implicit val AS = akka.actor.ActorSystem()

    val raw = randomData(0)
    val data2 = Await
      .result(
        Source
          .single(raw)
          .via(lame.BlockGzip())
          .map(_._1)
          .via(lame.BlockGunzip(emitFromOffsetOfFirstBlock = 0))
          .runWith(Sink.seq),
        Duration.Inf
      )
      .reduce(_ ++ _)

    raw shouldBe data2
    AS.terminate()
  }
  test("correctness - 90KB") {
    implicit val AS = akka.actor.ActorSystem()

    val raw = randomData(1024 * 90)
    val data2 = Await
      .result(
        Source
          .single(raw)
          .via(lame.BlockGzip())
          .map(_._1)
          .via(lame.BlockGunzip(emitFromOffsetOfFirstBlock = 0))
          .runWith(Sink.seq),
        Duration.Inf
      )
      .reduce(_ ++ _)

    raw shouldBe data2
    AS.terminate()
  }
  test("correctness - 270KB") {
    implicit val AS = akka.actor.ActorSystem()

    val raw = randomData(1024 * 90)
    val data2 = Await
      .result(
        Source
          .apply(List(raw, raw, raw))
          .via(lame.BlockGzip())
          .map(_._1)
          .via(lame.BlockGunzip(emitFromOffsetOfFirstBlock = 0))
          .runWith(Sink.seq),
        Duration.Inf
      )
      .reduce(_ ++ _)

    (raw ++ raw ++ raw) shouldBe data2
    AS.terminate()
  }
  test("correctness - 120KB") {
    implicit val AS = akka.actor.ActorSystem()

    val raw = randomData(1024 * 40)
    val data2 = Await
      .result(
        Source
          .apply(List(raw, raw, raw))
          .via(lame.BlockGzip())
          .map(_._1)
          .via(lame.BlockGunzip(emitFromOffsetOfFirstBlock = 0))
          .runWith(Sink.seq),
        Duration.Inf
      )
      .reduce(_ ++ _)

    (raw ++ raw ++ raw) shouldBe data2
    AS.terminate()
  }

  test("adjacent span") {
    implicit val AS = akka.actor.ActorSystem()

    val a1 = List("a", "a", "b", "c", "d", "e", "h", "h")
    val f =
      Await.result(
        Source(a1).via(BlockGzip.adjacentSpan(_.head)).runWith(Sink.seq),
        20 seconds
      )
    f should equal(
      List(
        List("a", "a"),
        List("b"),
        List("c"),
        List("d"),
        List("e"),
        List("h", "h")
      )
    )

    AS.terminate()

  }

}
