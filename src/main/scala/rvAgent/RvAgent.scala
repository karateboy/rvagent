package rvAgent

import akka.actor.{ Actor, ActorLogging, Props, ActorRef }
import akka.actor.ActorSystem
import scala.concurrent.ExecutionContext.Implicits.global
import com.typesafe.scalalogging._

object RvAgent extends LazyLogging {
  import com.typesafe.config.ConfigFactory
  case object ParseOutput

  val config = ConfigFactory.load()
  val subject = config.getString("subject")
  logger.info(s"rvAgent subject=$subject")

  val inputPath = config.getString("inputPath")
  logger.info(s"inputPath =$inputPath")

  var receiver: ActorRef = _
  def startup(system: ActorSystem) = {
    receiver = system.actorOf(Props(classOf[RvAgent]), name = "rvAgent")
  }

  def parseOutput = {
    receiver ! ParseOutput
  }

  import java.nio.file.{ Paths, Files, StandardOpenOption }
  import java.nio.charset.{ StandardCharsets }
  import scala.collection.JavaConverters._

  /*
  private val parsedFileName = "parsed.txt"
  private var parsedFileList =
    try {
      Files.readAllLines(Paths.get(parsedFileName), StandardCharsets.UTF_8).asScala.toSeq
    } catch {
      case ex: Throwable =>
        Console.println(s"failed to open $parsedFileName")
        Seq.empty[String]
    }

  def appendToParsedFileList(filePath: String) = {
    parsedFileList = parsedFileList ++ Seq(filePath)

    try {
      Files.write(Paths.get(parsedFileName), (filePath + "\n").getBytes(StandardCharsets.UTF_8), StandardOpenOption.CREATE, StandardOpenOption.APPEND)
    } catch {
      case ex: Throwable =>
        Console.println(ex.getMessage)
    }
  }
	*/
}

import scala.concurrent.{ Future, Promise }

class RvAgent extends Actor with LazyLogging {
  import RvAgent._
  import com.typesafe.config.ConfigFactory
  import com.tibco.tibrv._
  val tibrvVer = Tibrv.getVersion
  logger.info(s"Tibro ver=$tibrvVer")

  logger.info("Tibrv set to native")
  Tibrv.open(Tibrv.IMPL_NATIVE)
  logger.info(s"Tibrv valid=${Tibrv.isValid()}")
  val transport = new TibrvRvdTransport("8585", "", "tcp:8585")

  import com.github.nscala_time.time.Imports._
  def send(dt: DateTime, computer:String, channel:String, mtDataList: List[(String, String)]) {
    // open Tibrv in native implementation
    try {      
      val msg = new TibrvMsg()
      msg.setSendSubject(subject)
      msg.add("qpID", "2AGTA100")
      msg.add("ruleSrvName", "IC_RULEsrv")
      val userId = if(computer == "IC01")
        "T2IC01"
      else
        "T2TS01"
        
      msg.add("userId", userId)
      msg.add("STRMID", "2AGTA100_STR900")
      msg.add("STRMNO", "1")
      msg.add("STRMQTY", channel)

      val eapActionMsg = new TibrvMsg()
      eapActionMsg.add("class", "PDSGlassSend")
      eapActionMsg.add("tId", "18_2AGTA100_PDSGlass_15:33:33:859")
      eapActionMsg.add("lotId", "AAEE2A100A01")
      eapActionMsg.add("componentId", " AAEE2A100A01")
      eapActionMsg.add("lotType", "P")
      eapActionMsg.add("batchId", " BPIC0001")
      eapActionMsg.add("batchType", "P")
      eapActionMsg.add("samplingFlag", "N")
      eapActionMsg.add("abnormalFlag", "abnormal")
      eapActionMsg.add("panelSize", "24")
      eapActionMsg.add("modelName", "ABA")
      eapActionMsg.add("processMode", "Dummy")
      eapActionMsg.add("productId", "BAEJ2A")
      eapActionMsg.add("planId", "MT180EN01_TOP")
      eapActionMsg.add("stepId", " 1SD_IC_01")
      eapActionMsg.add("stepHandle", "X")
      eapActionMsg.add("recipeId", "X")
      eapActionMsg.add("eqpPPID", "X")
      eapActionMsg.add("sourceCarrierId", "09223")
      eapActionMsg.add("sourceSlotNo", "35")
      eapActionMsg.add("targetCarrierId", "09224")
      eapActionMsg.add("targetSlotNo", "22")
      eapActionMsg.add("edcPlanId", "")
      val chamberPath = if(computer == "IC01")
        "00IC001"
      else
        "00TS001"
        
      eapActionMsg.add("chamberPath", chamberPath)
      eapActionMsg.add("processQty", "7")

      val nowStr = DateTime.now().toString("YYYYMMdd HHmmss")
      eapActionMsg.add("trackInTime", nowStr)
      eapActionMsg.add("timestamp", dt.toString("YYYYMMdd HHmmss"))
      eapActionMsg.add("processUnit1", "2AGTA100,00IC001,X, 20170831 153000, 20170831 153100")
      val mtValStr = mtDataList.map(elm => elm._1 + "," + elm._2).mkString(",")
      eapActionMsg.add("processData1", mtValStr)
      msg.add("eapAction", eapActionMsg)
      transport.send(msg);
    } catch {
      case ex: TibrvException =>
        logger.error("failed to open Tibrv", ex)
    }
    logger.info("send complete")
  }

  def receive = {
    case ParseOutput =>
      try {
        processInputPath(parser)
      } catch {
        case ex: Throwable =>
          logger.error("processInputPath failed", ex)
      }
      import scala.concurrent.duration._

      context.system.scheduler.scheduleOnce(scala.concurrent.duration.Duration(1, scala.concurrent.duration.MINUTES), self, ParseOutput)
  }

  import java.io.File
  def parser(f: File): Boolean = {
    import java.nio.file.{ Paths, Files, StandardOpenOption }
    import java.nio.charset.{ StandardCharsets }
    import scala.collection.JavaConverters._

    val lines =
      try {
        Files.readAllLines(Paths.get(f.getAbsolutePath), StandardCharsets.ISO_8859_1).asScala
      } catch {
        case ex: Throwable =>
          logger.error("rvAgent", "failed to read all lines", ex)
          Seq.empty[String]
      }

    if (lines.isEmpty) {
      false
    } else {
      def recordParser(unparsed: scala.collection.Seq[String]): List[(DateTime, String, String, List[(String, String)])] = {
        if (unparsed.length < 2)
          Nil
        else {
          var lineNo = 0
          var dt: Option[DateTime] = None
          var data = List.empty[(String, String)]
          var computer = ""
          var channel = ""
          try {
            dt = Some(DateTime.parse(unparsed(lineNo), DateTimeFormat.forPattern("YYYY/MM/dd HH:mm")))
            lineNo += 1
            while (lineNo < unparsed.length) {
              val unparsed_line = unparsed(lineNo)
              val elements = unparsed_line.split(";").toList
              computer = elements(1)
              channel = elements(2)
              val mt = elements(3)
              val v = elements(4)
              logger.info(s"ch=$channel mt=$mt value=$v")
              data = data :+ (mt, v)
              lineNo += 1
            }
            (dt.get, computer, channel, data) :: Nil
          } catch {
            case _: Throwable =>
              if (dt.isDefined && !data.isEmpty)
                (dt.get, computer, channel, data) :: recordParser(unparsed.drop(lineNo))
              else
                recordParser(unparsed.drop(lineNo))
          }
        }
      }

      val records = recordParser(lines)
      logger.debug(s"record = ${records.length}")
      for (rec <- records) {
        send(rec._1, rec._2, rec._3, rec._4)
      }
      true
    }
  }

  def processInputPath(parser: (File) => Boolean) = {

    def listAllFiles = {
      //import java.io.FileFilter
      val path = new java.io.File(RvAgent.inputPath)
      if (path.exists() && path.isDirectory()) {
        val allFiles = new java.io.File(RvAgent.inputPath).listFiles().toList
        allFiles.filter(p => p != null)
      } else {
        logger.warn(s"invalid input path ${RvAgent.inputPath}")
        List.empty[File]
      }
    }

    val files = listAllFiles
    for (f <- files) {
      if (f.getName.endsWith("txt")) {
        logger.info(s"parse ${f.getName}")
        try {
          val result = parser(f)

          if (result) {
            logger.info(s"${f.getAbsolutePath} success.")
            f.delete()
          }
        } catch {
          case ex: Throwable =>
            logger.error("skip buggy file", ex)
        }
      } else {
        f.delete()
      }
    }
  }

  override def postStop = {
    transport.destroy()
    Tibrv.close()
  }
}