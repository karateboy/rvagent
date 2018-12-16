package rvAgent

import akka.actor.ActorSystem
import scala.concurrent.ExecutionContext.Implicits.global
import com.github.nscala_time.time.Imports._

object AppMain extends App {
  
  val system = ActorSystem("MyActorSystem")
    
  import scala.concurrent.Await
  import scala.concurrent.duration._
  RvAgent.startup(system)
  RvAgent.parseOutput
  Await.result(system.whenTerminated, Duration.Inf)
}