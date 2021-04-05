package lame.index

import intervaltree._
import akka.util.ByteString
import lame.ByteReader
import lame.BlockGunzip
import lame.BlockGzip

case class Index(
    tree: Tree[IntervalTree.IntervalTreeElement[Long, Index.Block]],
    length: Long
) {
  def query(queryIdx: Long): Option[Index.QueryResult] = {
    IntervalTree
      .lookup1(queryIdx, queryIdx + 1, tree)
      .headOption
      .map { case Index.Block(from, _, vfp) =>
        Index.QueryResult(vfp, queryIdx - from)
      }
  }
}

object Index {
  case class Block(from: Long, to: Long, virtualFilePointer: Long)
      extends LongInterval

  case class QueryResult(
      virtualFilePointer: Long,
      numberOfSkippableElements: Long
  )

  def blocks(data: ByteString): Iterator[Block] = {
    val byteReader = new ByteReader(data)
    Iterator
      .continually {
        val idx1 = byteReader.readLongLE()
        val idx2 = byteReader.readLongLE()
        val vfp = byteReader.readLongLE()
        val block = Block(idx1, idx2 + 1, vfp)
        block
      }
      .take(data.size / 24)
  }

  def length(data: ByteString) = {
    if (data.isEmpty) 0L
    else {
      val reader = new ByteReader(data)
      reader.skip(((data.size / 24) - 1) * 24 + 8)
      reader.readLongLE() + 1
    }
  }

  def concatenate(indices: Iterator[(Long, ByteString)]) = {

    def shiftIndex(
        fileOffSet: Long,
        indexOffSet: Long,
        data: ByteString
    ): ByteString =
      blocks(data)
        .map { case Block(fromIdx, toIdx, vfp) =>
          val f = fromIdx + indexOffSet
          val t = toIdx - 1 + indexOffSet
          val v = BlockGunzip.shiftFileOffset(vfp, fileOffSet)
          BlockGzip.int64LE(f) ++
            BlockGzip.int64LE(t) ++
            BlockGzip.int64LE(v)
        }
        .foldLeft(ByteString.empty)(_ ++ _)

    indices
      .foldLeft((0L, 0L, ByteString.empty)) {
        case ((lastFileOffSet, lastIdx, accum), (dataFileLength, indexData)) =>
          val shifted = shiftIndex(
            fileOffSet = lastFileOffSet,
            indexOffSet = lastIdx,
            data = indexData
          )
          val newFileOffset = lastFileOffSet + dataFileLength
          val newLastIdx = lastIdx + length(indexData)
          val newAccum = accum ++ shifted
          (newFileOffset, newLastIdx, newAccum)
      }
      ._3
  }

  def apply(data: ByteString): Index = {

    Index(IntervalTree.makeTree[Long, Block](blocks(data).toList), length(data))
  }
}
