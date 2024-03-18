import picocli.CommandLine
import picocli.CommandLine.{Command, Option}

@Command(name = "SimpleApp", mixinStandardHelpOptions = true, version = Array("SimpleApp 1.0"),
  description = Array("Processes user credentials."))
class SimpleApp extends Runnable {

  @Option(names = Array("-u", "--username"), description = Array("The user's username."))
  private var username: String = _

  @Option(names = Array("-p", "--password"), description = Array("The user's password."))
  private var password: String = _

  @Option(names = Array("-n", "--minSize"), description = Array("Minimum content size"))
  private var minSize: Integer = _

  @Option(names = Array("-x", "--maxSize"), description = Array("Maximum content size."))
  private var maxSize: Integer = _

  @Option(names = Array("-b", "--batchSize"), description = Array("Batch size for reading source db records size."))
  private var batchSize: Integer = _

  @Option(names = Array("-t", "--targetPartitions"), description = Array("Target count of partitions for analysis and save to solr"))
  private var tartgetPartitions: Integer = _

  override def run(): Unit = {
    println(s"Welcome, $username! Your password is $password")
  }
}

object ScalaPicocliExample {
  def main(args: Array[String]): Unit = {
    val exitCode = new CommandLine(new SimpleApp()).execute(args: _*)
    System.exit(exitCode)
    println("Done")
  }
}
