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
  if (Tibrv.isIPM()) {
    logger.info("Tibrv set to IPM")
    Tibrv.open("./tibrvipm.cfg")
  } else {
    logger.info("Tibrv set to native")
    Tibrv.open(Tibrv.IMPL_NATIVE)
  }
  logger.info(s"Tibrv valid=${Tibrv.isValid()}")
  val transport = new TibrvRvdTransport("8585", "", "tcp:8585")

  import com.github.nscala_time.time.Imports._
  def send(dt: DateTime) {
    // open Tibrv in native implementation
    try {
      val msg = new TibrvMsg()
      msg.setSendSubject(subject)
      msg.add("eqpID", "2AGTA100")
      msg.add("STRMID", "2AGTA100_STR900")
      msg.add("STRMNO", 1)
      msg.add("lotId", "")
      msg.add("lotType", "")
      msg.add("batchId", "")
      msg.add("batchType", "")
      msg.add("samplingFlag", "")
      msg.add("abnormalFlag", "")
      msg.add("panelSize", "")
      msg.add("modelName", "")
      msg.add("processMode", "")
      msg.add("productId", "")
      msg.add("planId", "")
      msg.add("stepId", "")
      msg.add("stepHandle", "")
      msg.add("recipeId", "")
      msg.add("edcPlanId", "")
      msg.add("chamberPath", "")
      val nowStr = DateTime.now().toString("YYYYMMdd HHmmss")
      msg.add("trackInTime", nowStr)
      msg.add("timestamp", dt.toString("YYYYMMdd HHmmss"))
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
        Files.readAllLines(Paths.get(f.getAbsolutePath), StandardCharsets.UTF_8).asScala.toSeq
      } catch {
        case ex: Throwable =>
          Seq.empty[String]
      }

    if (lines.isEmpty)
      false
    else {
      val dt = DateTime.parse(lines(0), DateTimeFormat.forPattern("YYYY/MM/dd HH:mm"))
      for (i <- 1 to lines.length-1) {
        val line = lines(i)
        val elements = line.split(";").toList
        val ch = elements(2)
        val mt = elements(3)
        val v = elements(4)
        logger.info(s"ch=$ch mt=$mt value=$v")
        send(dt)
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