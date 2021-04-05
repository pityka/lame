package lame
import org.scalatest.funsuite._
import org.scalatest.matchers.should._
import akka.stream.scaladsl._
import akka.util.ByteString
import scala.concurrent.Await
import scala.concurrent.duration._
import java.io.File
import java.io.ByteArrayOutputStream
import java.nio.channels.Channels

class GunzipSuite extends AnyFunSuite with Matchers {
  def randomData(max: Int) =
    Source
      .unfold(new scala.util.Random)(random =>
        Some(
          (
            random, {
              val buf = Array.fill[Byte](1024 * 8)(0)
              random.nextBytes(buf); buf
            }
          )
        )
      )
      .map(ByteString(_))
      .take(max.toLong)
      .via(lame.Gzip())

  def gzip(data: ByteString) = {
    val bos = new ByteArrayOutputStream(data.length)
    val gzip = new java.util.zip.GZIPOutputStream(bos)
    val writeableChannel = Channels.newChannel(gzip)
    data.asByteBuffers.foreach(writeableChannel.write)

    gzip.close
    val compressed = bos.toByteArray
    bos.close()
    compressed
  }

  test("correctness") {
    implicit val AS = akka.actor.ActorSystem()

    println("start")
    val data = Await.result(randomData(5).runWith(Sink.seq), Duration.Inf)
    val data2 = Await
      .result(
        Source(data)
          .via(lame.Gzip())
          .via(lame.Gunzip())
          .runWith(Sink.seq),
        Duration.Inf
      )
      .reduce(_ ++ _)
    println("end")

    data.reduce(_ ++ _) shouldBe data2
    AS.terminate()
  }
  test("correctness on block gzip") {
    implicit val AS = akka.actor.ActorSystem()

    println("start")
    val data = Await.result(randomData(5).runWith(Sink.seq), Duration.Inf)
    val data2 = Await
      .result(
        Source(data)
          .map { byteString =>
            ByteString(gzip(byteString))
          }
          .via(lame.Gunzip())
          .runWith(Sink.seq),
        Duration.Inf
      )
      .reduce(_ ++ _)
    println("end")

    data.reduce(_ ++ _) shouldBe data2
    AS.terminate()
  }

  test("fragmented ByteStrings input") {
    implicit val AS = akka.actor.ActorSystem()

    val file = new File("tmp.data").toPath
    Await.result(
      randomData(1024 * 30).runWith(FileIO.toPath(file)),
      Duration.Inf
    )
    println("start decompress")
    val t1 = System.nanoTime
    Await.result(
      FileIO
        .fromPath(file)
        .via(
          Flow[ByteString]
            .groupedWeightedWithin(maxWeight = 1024 * 1024 * 5, 5 seconds)(
              _.length.toLong
            )
            .map(bs => bs.reduce(_ ++ _).grouped(30).toList.reduce(_ ++ _))
        )
        .runWith(
          lame
            .Gunzip()
            .toMat(Sink.ignore)(Keep.right)
        ),
      Duration.Inf
    )
    println((System.nanoTime - t1) * 1e-9)
    println("done")
    AS.terminate()

  }
}
