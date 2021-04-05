package lame

import akka.NotUsed
import akka.stream.{Attributes, IOResult}
import akka.stream.scaladsl.{Flow, FileIO, Keep, Sink}
import java.nio.file.Path
import akka.stream.stage.{GraphStageLogic, InHandler, OutHandler}
import akka.util.ByteString
import lame.gzip.CompressionUtils.LinearGraphStage
import lame.gzip.DeflateCompressor
import java.util.zip.Deflater
import java.util.zip.CRC32
import scala.concurrent.{Future, ExecutionContext}
import akka.stream.FlowShape
import akka.stream.Outlet
import akka.stream.Inlet
import akka.stream.stage.GraphStage

object BlockGzip {

  val MaxUncompressed =
    65536 - 10 - 26 // 10 is GZIP overhead if data is uncompressable and gzip level is 0
  // 26 is gzip header + trailer
  val MaxCompressed = 65536 - 26 // 26 is gzip header + trailer

  case class State(
      data: ByteString,
      emit: ByteString,
      uncompressedBlockOffsets: List[Int],
      deflater: Deflater,
      crc32: CRC32
  ) {
    private val noCompressionDeflater = new Deflater(0, true)
    def update(bs: ByteString) =
      copy(
        data = data ++ bs,
        uncompressedBlockOffsets = data.size :: uncompressedBlockOffsets
      )
    def needsInput = data.size < MaxUncompressed
    def clear = copy(emit = ByteString.empty, uncompressedBlockOffsets = Nil)
    def compress =
      if (data.isEmpty)
        this
      else {
        def header(compressed: ByteString): ByteString =
          ByteString(
            0x1f, // ID1
            0x8b, // ID2
            8, // CM = Deflate
            4, // FLG
            0, // MTIME 1
            0, // MTIME 2
            0, // MTIME 3
            0, // MTIME 4
            0, // XFL
            0, // OS
            6, // XLEN 1
            0, // XLEN 2
            66, // SI1 B
            67, // SI2 C
            2, // subfield length 1
            0, // subfield length 2
            compressed.size + 25, // BSIZE 1
            (compressed.size + 25) >> 8 // BSIZE 2
          )

        val byteReader = new ByteReader(data)
        val input =
          byteReader.take(math.min(MaxUncompressed, byteReader.remainingSize))
        crc32.reset()
        crc32.update(input.toArray)
        val trailer: ByteString = {
          def int32(i: Int): ByteString =
            ByteString(i, i >> 8, i >> 16, i >> 24)
          val crc = crc32.getValue.toInt
          val trailer = int32(crc) ++ int32(input.size)

          trailer
        }

        val buffer =
          new Array[Byte](math.max(input.size, DeflateCompressor.MinBufferSize))

        val compressed = {
          deflater.reset()
          deflater.setInput(input.toArray)
          deflater.finish()
          val proposed =
            DeflateCompressor.drainDeflater(deflater, buffer, flush = true)
          if (proposed.size > MaxCompressed) {
            noCompressionDeflater.reset()
            noCompressionDeflater.setInput(input.toArray)
            noCompressionDeflater.finish()
            val bs = DeflateCompressor.drainDeflater(
              noCompressionDeflater,
              buffer,
              flush = true
            )
            bs
          } else proposed
        }

        val block = header(compressed) ++ compressed ++ trailer

        State(
          data = byteReader.remainingData,
          emit = emit ++ block,
          uncompressedBlockOffsets = uncompressedBlockOffsets,
          deflater = deflater,
          crc32 = crc32
        )

      }
  }

  private[lame] def int64LE(i: Long): ByteString =
    ByteString(i, i >> 8, i >> 16, i >> 24, i >> 32, i >> 40, i >> 48, i >> 56)

  def flowWithIndex[Mat2](
      indexSink: Sink[ByteString, Mat2],
      compressionLevel: Int = 1,
      customDeflater: Option[() => Deflater] = None
  ): Flow[ByteString, ByteString, Mat2] = {
    val indexSink2 = Flow[(ByteString, List[Long])]
      .mapConcat(_._2)
      .zipWithIndex
      .map { case (vfp, idx) =>
        val blockAddress = BlockGunzip.getFileOffset(vfp)
        (blockAddress, vfp, idx)
      }
      .via(adjacentSpan(_._1))
      .filter(_.nonEmpty)
      .map { block =>
        val (_, vfp1, idx1) = block.head
        val (_, _, idx2) = block.last
        int64LE(idx1) ++ int64LE(idx2) ++ int64LE(vfp1)
      }
      .toMat(indexSink)(Keep.both)

    apply(compressionLevel, customDeflater)
      .alsoToMat(indexSink2)(Keep.right)
      .map(_._1)
      .mapMaterializedValue(_._2)

  }

  def sinkWithIndex[Mat1, Mat2](
      dataSink: Sink[ByteString, Mat1],
      indexSink: Sink[ByteString, Mat2],
      compressionLevel: Int = 1,
      customDeflater: Option[() => Deflater] = None
  ): Sink[ByteString, (Mat1, Mat2)] =
    flowWithIndex(indexSink, compressionLevel, customDeflater)
      .toMat(dataSink)(Keep.both)
      .mapMaterializedValue { case (a, b) => (b, a) }

  def file(
      data: Path,
      index: Path,
      compressionLevel: Int = 1,
      customDeflater: Option[() => Deflater] = None
  )(implicit
      ec: ExecutionContext
  ): Sink[ByteString, Future[(IOResult, IOResult)]] =
    sinkWithIndex(
      dataSink = FileIO.toPath(data),
      indexSink = FileIO.toPath(index),
      compressionLevel,
      customDeflater
    ).mapMaterializedValue { case (f1, f2) =>
      for {
        r1 <- f1
        r2 <- f2
      } yield (r1, r2)
    }

  def sinkWithIndexAsByteString[Mat](
      data: Sink[ByteString, Mat],
      compressionLevel: Int = 1,
      customDeflater: Option[() => Deflater] = None
  )(implicit
      ec: ExecutionContext
  ): Sink[ByteString, Future[(Mat, ByteString)]] =
    sinkWithIndex(
      dataSink = data,
      indexSink = Sink.seq[ByteString],
      compressionLevel,
      customDeflater
    ).mapMaterializedValue { case (dataMat, futureIndex) =>
      for {
        indexBytes <- futureIndex
      } yield (dataMat, indexBytes.foldLeft(ByteString.empty)(_ ++ _))
    }

  def apply(
      compressionLevel: Int = 1,
      customDeflater: Option[() => Deflater] = None
  ): Flow[ByteString, (ByteString, List[Long]), NotUsed] =
    Flow.fromGraph {
      new LinearGraphStage[ByteString, (ByteString, List[Long])] {
        override def createLogic(
            inheritedAttributes: Attributes
        ): GraphStageLogic =
          new GraphStageLogic(shape) with InHandler with OutHandler {

            var state = State(
              ByteString.empty,
              ByteString.empty,
              Nil,
              customDeflater.getOrElse(() =>
                new Deflater(compressionLevel, true)
              )(),
              new CRC32
            )

            var compressedStreamOffset = 0L

            def makeFilePointers() =
              state.uncompressedBlockOffsets.reverse.map { off =>
                BlockGunzip.createVirtualFileOffset(compressedStreamOffset, off)
              }

            val bgzipTrailer = ByteString(
              0x1f, 0x8b, 0x08, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0xff, 0x06,
              0x00, 0x42, 0x43, 0x02, 0x00, 0x1b, 0x00, 0x03, 0x00, 0x00, 0x00,
              0x00, 0x00, 0x00, 0x00, 0x00, 0x00
            )

            override def onPush(): Unit = {
              state = state.update(grab(in))
              if (!state.needsInput) {
                state = state.compress
              }
              if (state.emit.nonEmpty && isAvailable(out)) {
                push(out, (state.emit, makeFilePointers()))
                compressedStreamOffset += state.emit.size
                state = state.clear
              }

              if (state.needsInput) {
                pull(in)
              }
            }

            override def onPull(): Unit = {
              if (state.emit.nonEmpty) {
                push(out, (state.emit, makeFilePointers()))
                compressedStreamOffset += state.emit.size
                state = state.clear
              }
              if (!hasBeenPulled(in) && state.needsInput) {
                pull(in)
              }
            }

            override def onUpstreamFinish(): Unit = {
              while (state.data.nonEmpty) {
                state = state.compress
              }
              if (state.emit.nonEmpty) {
                emit(out, (state.emit ++ bgzipTrailer, makeFilePointers()))
                compressedStreamOffset += (state.emit.size + bgzipTrailer.size)
                state = state.clear
              } else {
                emit(out, (bgzipTrailer, makeFilePointers()))
                compressedStreamOffset += bgzipTrailer.size
              }
              completeStage()
            }

            override def postStop(): Unit = state.deflater.end()

            setHandlers(in, out, this)
          }
      }
    }

  private[lame] def adjacentSpan[T, K](key: T => K): Flow[T, Seq[T], NotUsed] =
    Flow.fromGraph(new GraphStage[FlowShape[T, Seq[T]]] {
      val in: Inlet[T] = Inlet("lame.adjacentSpanIn")
      val out: Outlet[Seq[T]] = Outlet("lame.adjacentSpanOut")

      override val shape = FlowShape.of(in, out)

      override def createLogic(attr: Attributes): GraphStageLogic =
        new GraphStageLogic(shape) {

          val buffer = scala.collection.mutable.ArrayBuffer[T]()

          setHandler(
            in,
            new InHandler {
              override def onUpstreamFinish(): Unit = {
                emit(out, buffer.toList)
                complete(out)
              }
              override def onPush(): Unit = {
                val elem = grab(in)
                if (buffer.isEmpty || key(buffer.head) == key(elem)) {
                  buffer.append(elem)
                  pull(in)
                } else {
                  val k = buffer.toList
                  buffer.clear()
                  buffer.append(elem)
                  push(out, k)
                }
              }
            }
          )
          setHandler(
            out,
            new OutHandler {
              override def onPull(): Unit = {
                pull(in)
              }
            }
          )
        }
    })

}
