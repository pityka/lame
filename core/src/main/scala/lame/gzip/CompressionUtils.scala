/*
 * Copyright (C) 2009-2019 Lightbend Inc. <https://www.lightbend.com>
 *
 * Changes to the Akka file: do not syncflush on each push
 */

package lame.gzip

import akka.NotUsed
import akka.stream.{Attributes, Inlet, Outlet, FlowShape}
import akka.stream.scaladsl.Flow
import akka.stream.stage.{GraphStageLogic, InHandler, OutHandler, GraphStage}
import akka.util.ByteString
import akka.event.Logging

object CompressionUtils {

  abstract class SimpleLinearGraphStage[T] extends GraphStage[FlowShape[T, T]] {
    val in = Inlet[T](Logging.simpleName(this) + ".in")
    val out = Outlet[T](Logging.simpleName(this) + ".out")
    override val shape = FlowShape(in, out)
  }

  abstract class LinearGraphStage[I, O] extends GraphStage[FlowShape[I, O]] {
    val in = Inlet[I](Logging.simpleName(this) + ".in")
    val out = Outlet[O](Logging.simpleName(this) + ".out")
    override val shape = FlowShape(in, out)
  }

  /** Creates a flow from a compressor constructor.
    */
  def compressorFlow(
      newCompressor: () => Compressor
  ): Flow[ByteString, ByteString, NotUsed] =
    Flow.fromGraph {
      new SimpleLinearGraphStage[ByteString] {
        override def createLogic(
            inheritedAttributes: Attributes
        ): GraphStageLogic =
          new GraphStageLogic(shape) with InHandler with OutHandler {
            val compressor = newCompressor()
            var buffer = ByteString.empty

            override def onPush(): Unit = {
              buffer = buffer ++ compressor.compress(grab(in))
              if (buffer.nonEmpty && isAvailable(out)) {
                push(out, buffer)
                buffer = ByteString.empty
              }

              if (compressor.needsInput) {
                pull(in)
              }
            }

            override def onPull(): Unit = {
              if (buffer.nonEmpty) {
                push(out, buffer)
                buffer = ByteString.empty
              }
              if (!hasBeenPulled(in) && compressor.needsInput) {
                pull(in)
              }
            }

            override def onUpstreamFinish(): Unit = {
              val data = compressor.flush() ++ compressor.finish()
              if (buffer.nonEmpty || data.nonEmpty) {
                emit(out, buffer ++ data)
                buffer = ByteString.empty
              }
              completeStage()
            }

            override def postStop(): Unit = compressor.close()

            setHandlers(in, out, this)
          }
      }
    }
}
