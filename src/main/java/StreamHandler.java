import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

/**
 * author: Qiao Hongbo
 * time: {$time}
 **/
public class StreamHandler {
    public StreamHandler(){
        SparkConf conf = new SparkConf()
                .setMaster("spark://pyq-master:7077")
                .setAppName("Team13");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(1));

//        JavaReceiverInputDStream<String> lines = jssc.file
    }
}
