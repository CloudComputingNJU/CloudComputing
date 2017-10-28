import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.bson.Document;
import scala.Tuple2;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;

/**
 * author: Qiao Hongbo
 * time: {$time}
 **/
public class StreamHandlerSocket implements Serializable {
    public static final String STREAM_SERVER_HOST = "localhost";
    public static final int STREAM_SERVER_PORT = 9999;
    public boolean hasHandshake = false;

    public Socket initSocket() {
        Socket skt = null;
        try {
            skt = new ServerSocket(9090).accept();
            System.out.println("accepted: " + skt.getRemoteSocketAddress());
            final Writer writer = new PrintWriter(skt.getOutputStream());
            //获取socket输入流信息
            InputStream in = skt.getInputStream();
            //获取socket输出
            PrintWriter pw = new PrintWriter(skt.getOutputStream(), true);
            //读入缓存(定义一个1M的缓存区)
            byte[] buf = new byte[1024];
            //读到字节（读取输入流数据到缓存）
            int len = in.read(buf, 0, 1024);
            //读到字节数组（定义一个容纳数据大小合适缓存区）
            byte[] res = new byte[len];
            //将buf内中数据拷贝到res中
            System.arraycopy(buf, 0, res, 0, len);
            //打印res缓存内容
            String key = new String(res);

            if (!hasHandshake && key.indexOf("Key") > 0) {
                //握手
                //通过字符串截取获取key值
                key = key.substring(0, key.indexOf("==") + 2);
                key = key.substring(key.indexOf("Key") + 4, key.length()).trim();
                //拼接WEBSOCKET传输协议的安全校验字符串
                key += "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";
                //通过SHA-1算法进行更新
                MessageDigest md = null;
                try {
                    md = MessageDigest.getInstance("SHA-1");
                    md.update(key.getBytes("utf-8"), 0, key.length());

                } catch (NoSuchAlgorithmException e) {
                    e.printStackTrace();
                } catch (UnsupportedEncodingException e) {
                    e.printStackTrace();
                }
                byte[] sha1Hash = md.digest();
                //进行Base64加密
                sun.misc.BASE64Encoder encoder = new sun.misc.BASE64Encoder();
                key = encoder.encode(sha1Hash);
                //服务器端返回输出内容
                pw.println("HTTP/1.1 101 Switching Protocols");
                pw.println("Upgrade: websocket");
                pw.println("Connection: Upgrade");
                pw.println("Sec-WebSocket-Accept: " + key);
                pw.println();
                pw.flush();
                //将握手标志更新，只握一次
                hasHandshake = true;
                //服务器返回成功，---握手协议完成，进行TCP通信
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return skt;
    }

    public void start(Socket skt) throws IOException {
        SparkConf conf = new SparkConf()
//                .setMaster("spark://pyq-master:7077")
                .setMaster("local[2]")
                .set("spark.driver.host", "localhost")
                .setAppName("Team13");

        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(1));
//        jssc.sparkContext().addJar("/home/puyvqi/test/CloudComputing/target/spark-streaming-jingdong-1.0-SNAPSHOT.jar");
//        jssc.sparkContext().addJar("/home/puyvqi/test/CloudComputing/target/dependency/mongo-java-driver-3.4.2.jar");

        // 从socket读取流
        JavaReceiverInputDStream<String> lines = jssc.socketTextStream(STREAM_SERVER_HOST, STREAM_SERVER_PORT);

        // 转换为Document
        JavaDStream<Document> docs = lines.map(new Function<String, Document>() {
            @Override
            public Document call(String s) throws Exception {
                return Document.parse(s);
            }
        });

        JavaPairDStream<String, Integer> commentPairs = docs.mapToPair(new PairFunction<Document, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Document document) throws Exception {
                String name = (String) document.get("reference_name");
//                String brand = getBrand(name);
                return new Tuple2<>(getBrand(name), 1);
            }
        });


        JavaPairDStream<String, Integer> commentCounts = commentPairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        });
        JavaPairDStream<Integer, String> reverseCommentCounts = commentCounts.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
            @Override
            public Tuple2<Integer, String> call(Tuple2<String, Integer> commentCount) throws Exception {
                return commentCount.swap();
            }
        });

        JavaPairDStream<Integer, String> sortedReverseCommentCounts = reverseCommentCounts.transformToPair(new Function<JavaPairRDD<Integer, String>, JavaPairRDD<Integer, String>>() {
            @Override
            public JavaPairRDD<Integer, String> call(JavaPairRDD<Integer, String> reverse) throws Exception {
                return reverse.sortByKey(false);
            }
        });

        JavaPairDStream<String, Integer> sortedCommentCounts = sortedReverseCommentCounts.mapToPair(new PairFunction<Tuple2<Integer, String>, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Tuple2<Integer, String> commentCount) throws Exception {
                return commentCount.swap();
            }
        });


        JavaPairDStream<Integer, Integer> timestamps = docs.mapToPair(new PairFunction<Document, Integer, Integer>() {
            @Override
            public Tuple2<Integer, Integer> call(Document document) throws Exception {
                return new Tuple2<>((int) document.get("time_stamp"), 1);
            }
        });

        JavaPairDStream<Integer, Integer> sortedTimestamps = timestamps.transformToPair(new Function<JavaPairRDD<Integer, Integer>, JavaPairRDD<Integer, Integer>>() {
            @Override
            public JavaPairRDD<Integer, Integer> call(JavaPairRDD<Integer, Integer> timeStamp) throws Exception {
                return timeStamp.sortByKey(false);
            }
        });

        sortedCommentCounts.print(9);
//        sortedCommentCounts.saveAsHadoopFiles();
        sortedCommentCounts.foreachRDD(new VoidFunction<JavaPairRDD<String, Integer>>() {
            @Override
            public void call(JavaPairRDD<String, Integer> rankLine) throws Exception {
                List<Tuple2<String, Integer>> list = rankLine.collect();
                boolean finalFragment = true;
//                for (;;) {
                for (Tuple2<String, Integer> item : list) {
                    output(skt, finalFragment, item.toString() + "\r\n");
                }
                output(skt, finalFragment, "\r\n");
            }
        });
//        sortedCommentCounts.comp
//        sortedCommentCounts.saveAsHadoopFiles();
        jssc.start();
        try {
            jssc.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void output(Socket skt, boolean finalFragment, String str) {
        ByteBuffer byteBuf = ByteBuffer.wrap(str.getBytes());
        OutputStream out = null;
        try {
            out = skt.getOutputStream();
            int first = 0x00;
            //是否是输出最后的WebSocket响应片段
            if (finalFragment) {
                first = first + 0x80;
                first = first + 0x1;
            }
            out.write(first);
            if (byteBuf.limit() < 126) {
                out.write(byteBuf.limit());
            } else if (byteBuf.limit() < 65536) {
                out.write(126);
                out.write(byteBuf.limit() >>> 8);
                out.write(byteBuf.limit() & 0xFF);
            } else {
                // Will never be more than 2^31-1
                out.write(127);
                out.write(0);
                out.write(0);
                out.write(0);
                out.write(0);
                out.write(byteBuf.limit() >>> 24);
                out.write(byteBuf.limit() >>> 16);
                out.write(byteBuf.limit() >>> 8);
                out.write(byteBuf.limit() & 0xFF);
            }
            // Write the content
            out.write(byteBuf.array(), 0, byteBuf.limit());
            out.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    private String getBrand(String name) {
        String[][] brands = {{"联想", "lenovo"}, {"戴尔", "dell"}, {"华硕", "asus"},
                {"惠普", "hp"}, {"小米"}, {"宏碁", "acer"}, {"苹果", "apple", "macbook"}, {"华为", "huawei"},
                {"三星", "samsung"}, {"神舟", "hasee"}};
        String nameL = name.toLowerCase();
        for (String[] brandArray : brands) {
            for (String brand : brandArray) {
                if (nameL.contains(brand)) {
                    return brandArray[0];
                }
            }
        }
        return "其他";
    }

    public static void main(String[] args) {
        PropertyConfigurator.configure("conf/log4j.properties");
        Logger logger = Logger.getLogger(StreamHandlerLambda.class);
        logger.debug("debug");


        StreamHandlerSocket handler = new StreamHandlerSocket();

        try {
            Socket skt = handler.initSocket();
            System.out.println("socket init done");
            handler.start(skt);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
