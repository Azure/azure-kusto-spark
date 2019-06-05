import com.microsoft.kusto.spark.datasink.KustoSinkOptions
import com.microsoft.kusto.spark.datasource.KustoSourceOptions
import com.microsoft.kusto.spark.sql.extension.SparkExtension._
import org.apache.spark.SparkConf
import org.apache.spark.sql._

object SimpleKustoDataSink {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "Your HADOOP_HOME")
    val sparkConf = new SparkConf().set("spark.testing", "true")
      .set("spark.ui.enabled", "false")
      .setAppName("SimpleKustoDataSink")
      .setMaster("local[*]")
    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    val conf: Map[String, String] = Map(
      KustoSourceOptions.KUSTO_AAD_CLIENT_ID -> "Your Client ID",
      KustoSourceOptions.KUSTO_AAD_CLIENT_PASSWORD -> "Your secret",
      KustoSourceOptions.KUSTO_QUERY -> "Your Kusto query"
    )

    // Create a DF - read from a Kusto cluster
    val df = sparkSession.read.kusto("Your Kusto Cluster", "Your Kusto Database", "Your Kusto Query in KustoOptions.Kusto_Query", conf)
    df.show

    // Now write to a Kusto table
    df.write
      .format("com.microsoft.kusto.spark.datasource")
      .option(KustoSinkOptions.KUSTO_CLUSTER, "Your Kusto Cluster")
      .option(KustoSinkOptions.KUSTO_DATABASE, "Your Kusto Database")
      .option(KustoSinkOptions.KUSTO_TABLE, "Your Kusto Destination Table")
      .option(KustoSinkOptions.KUSTO_AAD_CLIENT_ID, "Your Client ID")
      .option(KustoSinkOptions.KUSTO_AAD_CLIENT_PASSWORD, "Your secret")
      .save()

    sparkSession.stop
  }
}
