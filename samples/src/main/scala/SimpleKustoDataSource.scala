import com.microsoft.kusto.spark.datasource.KustoSourceOptions
import com.microsoft.kusto.spark.sql.extension.SparkExtension._
import org.apache.spark.SparkConf
import org.apache.spark.sql._

object SimpleKustoDataSource {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "Your HADOOP_HOME")
    val sparkConf = new SparkConf().set("spark.testing", "true")
      .set("spark.ui.enabled", "false")
      .setAppName("SimpleKustoDataSource")
      .setMaster("local[*]")
    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    val conf: Map[String, String] = Map(
      KustoSourceOptions.KUSTO_AAD_CLIENT_ID -> "Your Client ID",
      KustoSourceOptions.KUSTO_AAD_CLIENT_PASSWORD -> "Your secret",
      KustoSourceOptions.KUSTO_QUERY -> "Your Kusto query",
      KustoSourceOptions.KUSTO_BLOB_STORAGE_ACCOUNT_NAME -> "Your blob storage account",
      KustoSourceOptions.KUSTO_BLOB_STORAGE_ACCOUNT_KEY -> "Your storage account key, Alternatively, SAS key can be used",
      KustoSourceOptions.KUSTO_BLOB_CONTAINER -> "Your blob storage container name"
    )
    val df = sparkSession.read.kusto("Your Kusto Cluster", "Your Kusto Database", "Your Kusto Query in KustoOptions.Kusto_Query", conf)
    df.show
    sparkSession.stop
  }

}
