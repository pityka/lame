package lame
import org.scalatest._
import akka.stream._
import akka.stream.scaladsl._
import akka.util.ByteString
import scala.concurrent.Await
import scala.concurrent.duration._

class FramingSuite extends FunSuite with Matchers {

  test("correctness") {
    implicit val AS = akka.actor.ActorSystem()
    implicit val mat = ActorMaterializer()
    val split = Await
      .result(
        Source(
          List(
            ByteString.empty,
            ByteString("a"),
            ByteString("\n\n"),
            ByteString("aaa"),
            ByteString("\n\n"),
            ByteString("aaa"),
            ByteString("aaa"),
            ByteString("\na")
          )
        ).via(
            lame.Framing.delimiter('\n', Int.MaxValue, allowTruncation = true)
          )
          .runWith(Sink.seq),
        Duration.Inf
      )

    split.map(_.utf8String) shouldBe Seq("a", "", "aaa", "", "aaaaaa", "a")
    AS.terminate
  }

}