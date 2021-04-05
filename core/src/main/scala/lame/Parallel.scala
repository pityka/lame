package lame

import scala.concurrent.{ExecutionContext, Future}
import akka.stream.scaladsl.Flow

object Parallel {
  def mapConcatAsync[T, K](parallelism: Int, bufferSize: Int = 256)(
      f: T => scala.collection.immutable.Iterable[K]
  )(implicit
      ec: ExecutionContext
  ): Flow[T, K, akka.NotUsed] =
    if (parallelism == 1)
      Flow[T].mapConcat(f)
    else
      Flow[T]
        .grouped(bufferSize)
        .mapAsync(parallelism) { lines =>
          Future {
            lines.flatMap(f)
          }(ec)
        }
        .mapConcat(identity)

  def mapAsync[T, K](parallelism: Int, bufferSize: Int = 256)(
      f: T => K
  )(implicit
      ec: ExecutionContext
  ): Flow[T, K, akka.NotUsed] =
    if (parallelism == 1)
      Flow[T].map(f)
    else
      Flow[T]
        .grouped(bufferSize)
        .mapAsync(parallelism) { lines =>
          Future {
            lines.map(f)
          }(ec)
        }
        .mapConcat(identity)
}
