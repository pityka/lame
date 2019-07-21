package lame.index

import org.scalatest._
import akka.stream._
import akka.stream.scaladsl._
import akka.util.ByteString
import scala.concurrent.Await
import scala.concurrent.duration._
import lame.BlockGzip
import scala.concurrent.ExecutionContext
import lame.BlockGunzip

class BlockGzipSuite extends FunSuite with Matchers {
  implicit val ec = ExecutionContext.global
  def randomData(size: Int) = {
    val random = new scala.util.Random
    val buf = Array.fill[Byte](size)(0)
    random.nextBytes(buf)
    ByteString(buf)
  }

  test("addressing - 3x 48KB") {
    implicit val AS = akka.actor.ActorSystem()
    implicit val mat = ActorMaterializer()
    val raw1 = randomData(1024 * 48)
    val raw2 = randomData(1024 * 48)
    val raw3 = randomData(1024 * 48)
    val raw4 = randomData(1024 * 48)
    val (compressedData, index) = Await
      .result(
        Source
          .apply(List(raw1, raw2, raw3, raw4))
          .runWith(
            BlockGzip
              .sinkWithIndexAsByteString(data = Sink.seq[ByteString])
              .mapMaterializedValue(
                _.flatMap {
                  case (f2, m2) => f2.map(m1 => (m1.reduce(_ ++ _), m2))
                }
              )
          ),
        Duration.Inf
      )

    val parsedIndex = Index(index)
    parsedIndex.length shouldBe 4
    parsedIndex.query(0L).get shouldBe 0L
    parsedIndex.query(1L).get shouldBe 0L
    parsedIndex.query(2L).get shouldBe 4295000100L
    parsedIndex.query(3L).get shouldBe 8589951048L
    parsedIndex.query(-1L) shouldBe None

    def get(i: Long) = {
      Await
        .result(
          BlockGunzip
            .sourceFromFactory(parsedIndex.query(i).get)(
              fileOffSet => Source.single(compressedData.drop(fileOffSet.toInt))
            )
            .runWith(Sink.seq),
          Duration.Inf
        )
        .reduce(_ ++ _)
        .take(1024 * 96)
    }

    get(0L).containsSlice(raw1) shouldBe true
    get(1L).containsSlice(raw2) shouldBe true
    get(2L).containsSlice(raw3) shouldBe true
    get(3L).containsSlice(raw4) shouldBe true

    AS.terminate()
  }

  test("concat - 3x 48KB") {
    implicit val AS = akka.actor.ActorSystem()
    implicit val mat = ActorMaterializer()
    val raw1 = randomData(1024 * 48)
    val raw2 = randomData(1024 * 48)
    val raw3 = randomData(1024 * 48)
    val raw4 = randomData(1024 * 48)
    val (compressedData, index) = Await
      .result(
        Source
          .apply(List(raw1, raw2, raw3, raw4))
          .runWith(
            BlockGzip
              .sinkWithIndexAsByteString(data = Sink.seq[ByteString])
              .mapMaterializedValue(
                _.flatMap {
                  case (f2, m2) => f2.map(m1 => (m1.reduce(_ ++ _), m2))
                }
              )
          ),
        Duration.Inf
      )

    val concatenatedIndex = Index.concatenate(
      List(
        (compressedData.length.toLong, index),
        (compressedData.length.toLong, index),
        (compressedData.length.toLong, index)
      ).iterator
    )

    val concatenetedCompressedData = compressedData ++ compressedData ++ compressedData

    val parsedIndex = Index(concatenatedIndex)
    parsedIndex.length shouldBe 12  
    parsedIndex.query(0L).get shouldBe 0L
    parsedIndex.query(1L).get shouldBe 0L
    parsedIndex.query(2L).get shouldBe 4295000100L
    parsedIndex.query(3L).get shouldBe 8589951048L
    parsedIndex.query(4L).isDefined shouldBe true
    parsedIndex.query(5L).isDefined shouldBe true
    parsedIndex.query(6L).isDefined shouldBe true
    parsedIndex.query(7L).isDefined shouldBe true
    parsedIndex.query(8L).isDefined shouldBe true
    parsedIndex.query(12L).isDefined shouldBe false
    parsedIndex.query(-1L) shouldBe None

    def get(i: Long) = {
      Await
        .result(
          BlockGunzip
            .sourceFromFactory(parsedIndex.query(i).get)(
              fileOffSet =>
                Source.single(concatenetedCompressedData.drop(fileOffSet.toInt))
            )
            .runWith(Sink.seq),
          Duration.Inf
        )
        .reduce(_ ++ _)
        .take(1024 * 96)
    }

    get(0L).containsSlice(raw1) shouldBe true
    get(1L).containsSlice(raw2) shouldBe true
    get(2L).containsSlice(raw3) shouldBe true
    get(3L).containsSlice(raw4) shouldBe true

    get(4L).containsSlice(raw1) shouldBe true
    get(5L).containsSlice(raw2) shouldBe true
    get(6L).containsSlice(raw3) shouldBe true
    get(7L).containsSlice(raw4) shouldBe true

    get(8L).containsSlice(raw1) shouldBe true
    get(9L).containsSlice(raw2) shouldBe true
    get(10L).containsSlice(raw3) shouldBe true
    get(11L).containsSlice(raw4) shouldBe true

    AS.terminate()
  }
}
