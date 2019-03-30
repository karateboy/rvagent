package rvAgent

import akka.actor.{ Actor, ActorLogging, Props, ActorRef }
import akka.actor.ActorSystem
import scala.concurrent.ExecutionContext.Implicits.global
import com.typesafe.scalalogging._
import scala.collection.JavaConverters._

object RvAgent extends LazyLogging {
  import com.typesafe.config.ConfigFactory
  case object ParseOutput

  val config = ConfigFactory.load()

  val inputPath = config.getString("inputPath")
  logger.info(s"inputPath =$inputPath")

  val ts01_config = config.getConfig("ts01_config")
  val ic01_config = config.getConfig("ic01_config")

  val ic01_props = config.getObject("ic01_prop")
  val ts01_props = config.getObject("ts01_prop")

  def getChannelMap(name: String) = {
    val channels = config.getObject(name).entrySet()
    val channelKV = channels.asScala map { ch =>
      val v = ch.getValue.render()
      (ch.getKey, v.substring(1, v.length() - 1))
    }
    channelKV.toMap
  }

  val ic01_channelMap = getChannelMap("ic01_channel")
  val ts01_channelMap = getChannelMap("ts01_channel")

  def getAnMap(name: String) = {
    val ans = config.getObject(name).entrySet()
    val anKV = ans.asScala map { an =>
      val v = an.getValue.render()
      (an.getKey, v.substring(1, v.length() - 1))
    }
    anKV.toMap
  }

  val ic01_anMap = getAnMap("ic01_an")
  val ts01_anMap = getAnMap("ts01_an")

  var receiver: ActorRef = _
  def startup(system: ActorSystem) = {
    receiver = system.actorOf(Props(classOf[RvAgent]), name = "rvAgent")
  }

  def parseOutput = {
    receiver ! ParseOutput
  }
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
  //val transport = new TibrvRvdTransport("8585", "", "tcp:8585")
  val ic01_transport = new TibrvRvdTransport(ic01_config.getString("Service"), "", ic01_config.getString("Daemon"))
  val ts01_transport = new TibrvRvdTransport(ts01_config.getString("Service"), "", ts01_config.getString("Daemon"))

  import com.github.nscala_time.time.Imports._

  var seq = 1
  def send2(dt: DateTime, computer: String, channel: String, mtDataList: List[(String, String)]) {
    // open Tibrv in native implementation
    try {

      def choose[T](v1: T, v2: T) =
        if (computer == "IC01")
          v1
        else
          v2

      val transport = choose(ic01_transport, ts01_transport)
      val config = choose(ic01_config, ts01_config)
      val channelMap = choose(ic01_channelMap, ts01_channelMap)
      val props = choose(ic01_props, ts01_props)
      val anMap = choose(ic01_anMap, ts01_anMap)

      val msg = new TibrvMsg()
      
      msg.setSendSubject(config.getString("Subject"))          
      val msgHeader = ">>L FwEapComplexTxn msgTag=FwEapComTxn "
      msg.add(s"$msgHeader eqpID", config.getString("eqpID"))
      msg.add("ruleSrvName", config.getString("ruleSrvName"))
      msg.add("userId", config.getString("userId"))
      val strmid_fmt = choose("%02d_2AGTA100_EapGlassDataReportInt_%s", "%02d_2AGTS100_EapGlassDataReportInt_%s")
      val strmid = strmid_fmt.format(seq, dt.toString("HH:mm:ss:000"))
      seq += 1
      msg.add("STRMID", strmid)
      msg.add("STRMNO", config.getString("STRMNO"))
      msg.add("STRMQTY", "1")

      val eapActionMsg = new TibrvMsg()
      val classHeader = "class=PDSGlassSend"
      eapActionMsg.add(s"$classHeader tId", strmid_fmt.format(seq, dt.toString("HH:mm:ss:000")))
      for (p <- props.entrySet().asScala) {
        val v = p.getValue.render()
        eapActionMsg.add(p.getKey, v.substring(1, v.length() - 1))
      }

      val nowStr = DateTime.now().toString("YYYYMMdd HHmmss")
      eapActionMsg.add("trackInTime", nowStr)
      val tsStr = dt.toString("YYYYMMdd HHmmss")
      eapActionMsg.add("timestamp", tsStr)

      eapActionMsg.add(
        "processUnit1",
        choose(s"${channelMap(channel)},,IC01, $tsStr, $nowStr", s"${channelMap(channel)},,TS01, $tsStr, $nowStr"))

      val mtValStr = mtDataList.map(elm => anMap(elm._1) + "=" + elm._2).mkString(",")
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
            case ex: java.lang.IndexOutOfBoundsException =>
              if (dt.isDefined && !data.isEmpty)
                (dt.get, computer, channel, data) :: recordParser(unparsed.drop(lineNo))
              else
                recordParser(unparsed.drop(lineNo))

            case ex: Throwable =>
              logger.error("unexpected error", ex)
              recordParser(Seq.empty[String])
          }
        }
      }

      val records = recordParser(lines)
      logger.info(s"record = ${records.length}")
      for (rec <- records) {
        send2(rec._1, rec._2, rec._3, rec._4)
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
    ts01_transport.destroy()
    ic01_transport.destroy()
    Tibrv.close()
  }
}