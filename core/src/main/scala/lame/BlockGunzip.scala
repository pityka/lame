package lame

import akka.util.ByteString
import akka.NotUsed
import akka.stream.scaladsl.{Flow, Source}
import scala.annotation.tailrec
import java.util.zip.{CRC32, Inflater, ZipException}
import scala.collection.mutable
import java.nio.file.Path
import akka.stream.scaladsl.FileIO
import scala.concurrent.Future

object BlockGunzip {

  val MaxBytesPerChunk = 65536

  object State {

    def apply(emitFromOffset: Int): State =
      State(
        data = ByteString.empty,
        emit = mutable.ArrayBuffer.empty,
        inflater = new Inflater(true),
        crc32 = new CRC32,
        emitFromOffset = emitFromOffset
      )

  }

  case class State(
      data: ByteString,
      emit: mutable.ArrayBuffer[ByteString],
      inflater: Inflater,
      crc32: CRC32,
      emitFromOffset: Int
  ) {

    override def toString =
      s"State($data, ${emit.size}, $inflater, $crc32)"

    def update(bs: ByteString): State = {
      copy(data = data ++ bs)
    }

  }

  private def fail(msg: String) = throw new ZipException(msg)

  private def inflate(
      compressedData: ByteString,
      inflater: Inflater,
      crc32: CRC32
  ) = {

    inflater.setInput(compressedData.toArray)

    val outputBuffer = new Array[Byte](MaxBytesPerChunk)
    val read = inflater.inflate(outputBuffer)
    crc32.update(outputBuffer, 0, read)
    assert(inflater.needsInput)

    ByteString.fromArray(outputBuffer, 0, read)

  }

  def readBlock(currentState: State) =
    if (currentState.data.isEmpty) currentState
    else {
      val reader = new ByteReader(currentState.data)
      import reader._
      try {
        /* The following lines parsing the gzip header bear the copyright of:
         * Copyright (C) 2009-2019 Lightbend Inc. <https://www.lightbend.com>
         */
        if (readByte() != 0x1f || readByte() != 0x8b)
          fail("Not in GZIP format") // check magic header
        if (readByte() != 8)
          fail(
            "Unsupported GZIP compression method"
          ) // check compression method
        val flags = readByte()
        skip(6) // skip MTIME, XFL and OS fields
        if (flags != 4)
          fail(
            "Not a valid BGZIP header. All flags must be 0 except F.EXTRA which must be 1"
          )
        val xLen = readShortLE()
        /* End Lightbend copyright */
        val extraFields = take(xLen)
        val extraFieldsByteReader = new ByteReader(extraFields)

        @tailrec
        def findBSize: Int = {
          if (!extraFieldsByteReader.hasRemaining)
            fail(
              "Not a valid BGZIP header. Needs extra subfield BC with payload of total block size"
            )
          else {
            if (
              extraFieldsByteReader
                .readByte() != 0x42 || extraFieldsByteReader
                .readByte() != 0x43 || extraFieldsByteReader
                .readShortLE() != 2
            ) findBSize
            else extraFieldsByteReader.readShortLE()
          }
        }

        val totalBockSize = findBSize
        val compressedDataSize = totalBockSize - xLen - 19
        val compressedData = take(compressedDataSize)

        currentState.inflater.reset()
        currentState.crc32.reset()

        val decompressed: ByteString =
          inflate(compressedData, currentState.inflater, currentState.crc32)

        if (readIntLE() != currentState.crc32.getValue.toInt)
          fail("Corrupt data (CRC32 checksum error)")
        if (readIntLE() != decompressed.size)
          fail("Corrupt GZIP trailer ISIZE")

        if (currentState.emitFromOffset > 0)
          currentState.emit.append(
            decompressed.drop(currentState.emitFromOffset)
          )
        else currentState.emit.append(decompressed)

        State(
          data = reader.remainingData,
          emit = currentState.emit,
          inflater = currentState.inflater,
          crc32 = currentState.crc32,
          emitFromOffset = 0
        )
      } catch {
        case ByteReader.NeedMoreData => currentState
      }

    }
  import akka.stream.stage._
  import akka.stream._

  class BlockGunzipStage(
      emitFromOffset: Int,
      inflater: Option[() => Inflater] = None
  ) extends GraphStage[FlowShape[ByteString, ByteString]] {

    val in = Inlet[ByteString]("lame.blockgunzip.in")
    val out = Outlet[ByteString]("lame.blockgunzip.out")

    val shape = FlowShape.of(in, out)

    override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
      new GraphStageLogic(shape) {

        var state = State(emitFromOffset = emitFromOffset)

        if (inflater.isDefined) {
          state = state.copy(inflater = inflater.get.apply())
        }

        override def postStop() = state.inflater.end()

        setHandler(
          in,
          new InHandler {
            override def onPush(): Unit = {
              val byteString = grab(in)
              state = state.update(byteString)
              state = readBlock(state)
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
              if (state.data.isEmpty && state.emit.isEmpty) {
                complete(out)
              }
            }

          }
        )

        setHandler(
          out,
          new OutHandler {
            override def onPull(): Unit = {
              if (state.data.isEmpty) {
                pull(in)
              }
              state = readBlock(state)
              if (state.emit.nonEmpty) {
                val emit = state.emit.toList
                state.emit.clear()
                emitMultiple(out, emit)
              }
              if (isClosed(in) && state.data.isEmpty && state.emit.isEmpty) {
                complete(out)
              }

            }
          }
        )
      }
  }

  def apply(
      emitFromOffsetOfFirstBlock: Int,
      customInflater: Option[() => Inflater] = None
  ): Flow[ByteString, ByteString, NotUsed] =
    Flow[ByteString].via(
      new BlockGunzipStage(emitFromOffsetOfFirstBlock, customInflater)
    )

  def sourceFromFile(
      path: Path,
      virtualFilePointer: Long = 0L,
      chunkSize: Int = 8192,
      customInflater: Option[() => Inflater] = None
  ): Source[ByteString, Future[akka.stream.IOResult]] = {
    val fileOffset = getFileOffset(virtualFilePointer)
    val blockOffset = getBlockOffset(virtualFilePointer)
    FileIO
      .fromPath(path, chunkSize = chunkSize, startPosition = fileOffset)
      .via(BlockGunzip(blockOffset, customInflater))
  }

  def sourceFromFactory[M](
      virtualFilePointer: Long = 0L,
      customInflater: Option[() => Inflater] = None
  )(
      mkSourceFromFileOffset: Long => Source[ByteString, M]
  ): Source[ByteString, Future[M]] = {
    val fileOffset = getFileOffset(virtualFilePointer)
    val blockOffset = getBlockOffset(virtualFilePointer)
    Source
      .lazySource(() => mkSourceFromFileOffset(fileOffset))
      .via(BlockGunzip(blockOffset, customInflater))
  }

  val AddressMask = 0xffffffffffffL
  val OffsetMask = 0xffff
  val MaxFileOffset = AddressMask
  val MaxBlockOffset = OffsetMask

  def getFileOffset(virtualFilePointer: Long) =
    (virtualFilePointer >> 16) & AddressMask

  def getBlockOffset(virtualFilePointer: Long): Int =
    (virtualFilePointer & OffsetMask).toInt

  def shiftFileOffset(virtualFilePointer: Long, shift: Long) = {
    val currentFileOffset = getFileOffset(virtualFilePointer)
    val currentBlockOffset = getBlockOffset(virtualFilePointer)
    createVirtualFileOffset(currentFileOffset + shift, currentBlockOffset)
  }

  def createVirtualFileOffset(fileOffset: Long, blockOffset: Int) = {
    if (blockOffset < 0) {
      throw new RuntimeException(
        s"Negative blockOffset $blockOffset not allowed."
      )
    }
    if (fileOffset < 0) {
      throw new RuntimeException(
        s"Negative fileOffset $fileOffset not allowed."
      )
    }
    if (blockOffset > MaxBlockOffset) {
      throw new RuntimeException(
        s"blockOffset $blockOffset too large."
      )
    }
    if (fileOffset > MaxFileOffset) {
      throw new RuntimeException(
        s"blockAddress $fileOffset too large."
      )
    }

    fileOffset << 16 | blockOffset

  }

}
