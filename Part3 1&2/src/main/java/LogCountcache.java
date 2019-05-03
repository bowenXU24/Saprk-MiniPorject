
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import java.util.concurrent.TimeUnit;


public class LogCountcache {
  public static void main(String[] args) {
    long startTime = System.nanoTime();

    String logFile = "sparkinput/access_log"; // Should be some file on your system
    SparkSession spark = SparkSession.builder().appName("Simple Application").getOrCreate();
    Dataset<String> logData = spark.read().textFile(logFile).cache();

    long numAs = logData.filter(s -> s.contains("/assets/img/loading.gif")).count();
    long numBs = logData.filter(s -> s.contains("/assets/js/lightbox.js")).count();

    System.out.println("Hits to loading.gif: " + numAs);
    System.out.println("Hits to lightbox.js: " + numBs);

    long endTime = System.nanoTime();
    long timeElapsed = endTime - startTime;
    System.out.println("Execution time with cache RDD: " + timeElapsed/1000);


    spark.stop();
  }
}
