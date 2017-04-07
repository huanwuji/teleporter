package teleporter.integration.component

import java.io.{InputStream, InputStreamReader}
import java.util

import akka.NotUsed
import akka.stream._
import akka.stream.scaladsl.Flow
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import akka.util.ByteString
import org.apache.commons.csv.{CSVFormat, CSVParser, CSVRecord}

/**
  * Created by huanwuji 
  * date 2017/2/24.
  */
object CSV {
  def flow(format: CSVFormat, maximumLineBytes: Int): Flow[ByteString, CSVRecord, NotUsed] = {
    Flow.fromGraph(new CSVStage(format, maximumLineBytes))
  }
}

/**
  * I want to use standard lib process csv file, but I don't want rewrite commons-csv to stream mode,
  * So use maximumLineBytes to adapt, It's not a good way. Thought pre-read avoid read stream
  * overload maximumLineBytes size is not safe
  */
class CSVStage(format: CSVFormat, val maximumLineBytes: Int) extends GraphStage[FlowShape[ByteString, CSVRecord]] {
  val in: Inlet[ByteString] = Inlet[ByteString]("CsvStage.in")
  val out: Outlet[CSVRecord] = Outlet[CSVRecord]("CsvStage.out")
  override val shape: FlowShape[ByteString, CSVRecord] = FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) with InHandler with OutHandler {
    private var buffer = ByteString.empty
    private var cursor = -1
    private val reader = new InputStreamReader(new InputStream {
      override def read(): Int = {
        cursor += 1
        require(cursor < buffer.length, s"cursor must little than ${buffer.length}, Please increase $maximumLineBytes")
        buffer(cursor)
      }
    })
    private val csvRecords: util.Iterator[CSVRecord] = new CSVParser(reader, format).iterator()

    override def onPush(): Unit = {
      buffer ++= grab(in)
      if (buffer.length < maximumLineBytes) {
        pull(in)
      } else {
        if (csvRecords.hasNext) {
          push(out, csvRecords.next())
          buffer = buffer.drop(cursor).compact
          cursor = -1
        }
      }
    }

    override def onPull(): Unit = {
      pull(in)
    }

    override def onUpstreamFinish(): Unit = {
      if (buffer.isEmpty) {
        completeStage()
      } else if (isAvailable(out)) {
        while (csvRecords.hasNext) {
          emit(out, csvRecords.next())
        }
        completeStage()
      }
    }

    setHandlers(in, out, this)
  }
}