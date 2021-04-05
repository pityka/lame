package lame

import akka.util.ByteString
import akka.NotUsed
import akka.stream.scaladsl.Flow
import scala.annotation.tailrec
import java.util.zip.{CRC32, Inflater, ZipException}
import scala.collection.mutable

object Gunzip {

  case class Configuration(
      maxBytesPerChunk: Int,
      expectedCompressionRatio: Double
  )

  object GunzipState {
    sealed trait Phase
    case object ReadHeader extends Phase
    case object Inflate extends Phase
    case object ReadTrailer extends Phase

    def apply(
        maxBytesPerChunk: Int,
        expectedCompressionRatio: Double
    ): GunzipState =
      GunzipState(
        dataHead = ByteString.empty,
        dataTail = ByteString.empty,
        phase = ReadHeader,
        emit = mutable.ArrayBuffer.empty,
        inflater = new Inflater(true),
        crc32 = new CRC32,
        config = Configuration(maxBytesPerChunk, expectedCompressionRatio)
      )

  }

  case class GunzipState(
      dataHead: ByteString,
      dataTail: ByteString,
      phase: GunzipState.Phase,
      emit: mutable.ArrayBuffer[ByteString],
      inflater: Inflater,
      crc32: CRC32,
      config: Configuration
  ) {

    override def toString =
      s"GunzipState($dataHead, $dataTail, $phase, ${emit.size}, $inflater, $crc32, $config)"

    def update(bs: ByteString): GunzipState = {
      copy(dataTail = dataTail ++ bs)
    }

    def inputIsEmpty = dataHead.isEmpty && dataTail.isEmpty

  }

  private def fail(msg: String) = throw new ZipException(msg)
  private def crc16(data: ByteString) = {
    val crc = new CRC32
    data.asByteBuffers.foreach(crc.update)
    crc.getValue.toInt & 0xffff
  }

  @tailrec
  def cyclePhases(currentState: GunzipState): GunzipState = {
    if (currentState.inputIsEmpty) currentState
    else
      currentState.phase match {
        case GunzipState.ReadHeader =>
          cyclePhases(readHeader(currentState))
        case GunzipState.Inflate =>
          if (currentState.emit.nonEmpty) currentState
          else cyclePhases(inflate(currentState))
        case GunzipState.ReadTrailer =>
          cyclePhases(readTrailer(currentState))
      }
  }

  def inflate(currentState: GunzipState) = {
    import currentState.config.{maxBytesPerChunk, expectedCompressionRatio}
    val (reader, tail) = {
      if (currentState.dataHead.nonEmpty)
        (new ByteReader(currentState.dataHead), currentState.dataTail)
      else {
        val effectiveExpectedCompressionRatio =
          math.max(1 / 1032d, math.min(1.0, expectedCompressionRatio))
        val headSize = math.max(
          1,
          (effectiveExpectedCompressionRatio * maxBytesPerChunk).toInt
        )
        val (head, tail) = currentState.dataTail.splitAt(headSize)
        (new ByteReader(head), tail)
      }
    }

    import currentState.inflater
    import currentState.crc32
    /* The following lines feeding the inflater and crc32 bear the copyright of:
     * Copyright (C) 2009-2019 Lightbend Inc. <https://www.lightbend.com>
     */
    inflater.setInput(reader.remainingData.toArray)

    val outputBuffer = new Array[Byte](maxBytesPerChunk)
    val read = inflater.inflate(outputBuffer)
    crc32.update(outputBuffer, 0, read)

    reader.skip(reader.remainingSize - inflater.getRemaining)

    currentState.emit.append(ByteString.fromArray(outputBuffer, 0, read))
    /* Lightbend copyright end */
    if (inflater.finished)
      GunzipState(
        dataHead = reader.remainingData,
        dataTail = tail,
        phase = GunzipState.ReadTrailer,
        emit = currentState.emit,
        inflater = inflater,
        crc32 = crc32,
        config = currentState.config
      )
    else
      GunzipState(
        dataHead = reader.remainingData,
        dataTail = tail,
        phase = GunzipState.Inflate,
        emit = currentState.emit,
        inflater = inflater,
        crc32 = crc32,
        config = currentState.config
      )

  }

  def readTrailer(currentState: GunzipState) = {
    val reader = new ByteReader(currentState.dataHead ++ currentState.dataTail)
    import reader._
    try {
      /* The following lines parsing the gzip trailer bear the copyright of:
       * Copyright (C) 2009-2019 Lightbend Inc. <https://www.lightbend.com>
       */
      if (readIntLE() != currentState.crc32.getValue.toInt)
        fail("Corrupt data (CRC32 checksum error)")
      if (
        readIntLE() != currentState.inflater.getBytesWritten.toInt /* truncated to 32bit */
      )
        fail("Corrupt GZIP trailer ISIZE")

      currentState.inflater.reset()
      currentState.crc32.reset()
      /* Lightbend copyright end */
      GunzipState(
        dataHead = ByteString.empty,
        dataTail = reader.remainingData,
        phase = GunzipState.ReadHeader,
        emit = currentState.emit,
        inflater = currentState.inflater,
        crc32 = currentState.crc32,
        config = currentState.config
      )
    } catch {
      case ByteReader.NeedMoreData => currentState
    }

  }

  def readHeader(currentState: GunzipState) = {
    val reader = new ByteReader(currentState.dataHead ++ currentState.dataTail)
    import reader._
    try {
      /* The following lines parsing the gzip header bear the copyright of:
       * Copyright (C) 2009-2019 Lightbend Inc. <https://www.lightbend.com>
       */
      if (readByte() != 0x1f || readByte() != 0x8b)
        fail("Not in GZIP format") // check magic header
      if (readByte() != 8)
        fail("Unsupported GZIP compression method") // check compression method
      val flags = readByte()
      skip(6) // skip MTIME, XFL and OS fields
      if ((flags & 4) > 0) skip(readShortLE()) // skip optional extra fields
      if ((flags & 8) > 0) skipZeroTerminatedString() // skip optional file name
      if ((flags & 16) > 0)
        skipZeroTerminatedString() // skip optional file comment
      if ((flags & 2) > 0 && crc16(fromStartToHere) != readShortLE())
        fail("Corrupt GZIP header")
      /* Lightbend copyright end */
      GunzipState(
        dataHead = ByteString.empty,
        dataTail = reader.remainingData,
        phase = GunzipState.Inflate,
        emit = currentState.emit,
        inflater = currentState.inflater,
        crc32 = currentState.crc32,
        config = currentState.config
      )
    } catch {
      case ByteReader.NeedMoreData => currentState
    }

  }
  import akka.stream.stage._
  import akka.stream._
  class GunzipStage(
      maxBytesPerChunk: Int = 65536,
      expectedCompressionRatio: Double = 1.0,
      inflater: Option[() => Inflater] = None
  ) extends GraphStage[FlowShape[ByteString, ByteString]] {

    val in = Inlet[ByteString]("lame.gunzip.in")
    val out = Outlet[ByteString]("lame.gunzip.out")

    val shape = FlowShape.of(in, out)

    override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
      new GraphStageLogic(shape) {

        var state = GunzipState(
          maxBytesPerChunk = maxBytesPerChunk,
          expectedCompressionRatio = expectedCompressionRatio
        )

        if (inflater.isDefined) {
          state = state.copy(inflater = inflater.get.apply())
        }

        override def postStop(): Unit = state.inflater.end()

        setHandler(
          in,
          new InHandler {
            override def onPush(): Unit = {
              val byteString = grab(in)
              state = state.update(byteString)
              state = cyclePhases(state)
              if (isAvailable(out) && state.emit.nonEmpty) {
                val emit = state.emit.toList
                state.emit.clear()
                emitMultiple(out, emit)
              } else {
                if (state.emit.isEmpty) {
                  pull(in)
                }
              }
            }

            override def onUpstreamFinish(): Unit = {
              if (state.inputIsEmpty && state.emit.isEmpty) {
                complete(out)
              }
            }

          }
        )

        setHandler(
          out,
          new OutHandler {
            override def onPull(): Unit = {
              if (state.inputIsEmpty) {
                pull(in)
              }
              state = cyclePhases(state)
              if (state.emit.nonEmpty) {
                val emit = state.emit.toList
                state.emit.clear()
                emitMultiple(out, emit)
              }
              if (isClosed(in) && state.inputIsEmpty && state.emit.isEmpty) {
                complete(out)
              }

            }
          }
        )
      }
  }

  def apply(
      maxBytesPerChunk: Int = 65536,
      expectedCompressionRatio: Double = 1.0,
      customInflater: Option[() => Inflater] = None
  ): Flow[ByteString, ByteString, NotUsed] =
    Flow[ByteString].via(
      new GunzipStage(
        maxBytesPerChunk,
        expectedCompressionRatio,
        customInflater
      )
    )

}
