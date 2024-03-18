//package com.oconeco.sparktest
//import cats.effect.{ExitCode, IO, IOApp}
//import cats.implicits.catsSyntaxTuple2Semigroupal
//import com.monovore.decline._
//
//object SimpleCLIApp extends IOApp {
//
//  private val usernameOpt = Opts.option[String]("username", help = "The user's username.")
//  private val passwordOpt = Opts.option[String]("password", help = "The user's password.", metavar = "secret")
//
//  private val greetingCommand = Command(name = "greet", header = "Greets the user with the provided username and password.") {
//    (usernameOpt, passwordOpt).mapN { (username, password) =>
//      println(s"Welcome, $username! Your password is $password")
//    }
//  }
//
//  override def run(args: List[String]): IO[ExitCode] = {
//    greetingCommand.parse(args) match {
//      case Left(help) =>
//        IO(System.err.println(help)).as(ExitCode.Error)
//      case Right(run) =>
//        IO(run).as(ExitCode.Success)
//    }
//  }
//}
