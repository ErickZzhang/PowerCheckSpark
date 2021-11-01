package paper;
import bean.PowerBean;
import bean.SourceBean;
import com.alibaba.fastjson.JSON;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import utils.InfluxDBSink;
import utils.MathUtils;

import java.util.Properties;

public class PowerCheck {
    public static void main(String[] args) throws Exception {

        SparkSession spark = SparkSession
                .builder()
                .appName("PowerCheckSpark")
                .getOrCreate();
//        Properties prop = new Properties();
//        prop.setProperty("source.topic", "PMU-data");
//        prop.setProperty("bootstrap.servers", "10.66.101.210:31090,10.66.101.210:31091,10.66.101.210:31092");
//        prop.setProperty("group.id", "demo1");
//        prop.setProperty("auto.offset.reset", "earliest");
//        prop.setProperty("enable.auto.submit","true");
//        prop.setProperty("jobName", "defaultJobName");
//        prop.setProperty("kafkaParallelism","3");

        Dataset<Row> lines = spark.readStream().format("kafka")
                .option("kafka.bootstrap.servers", "kafka-1.kafka-headless.default.svc.cluster.local:9092,kafka-2.kafka-headless.default.svc.cluster.local:9092,kafka-3.kafka-headless.default.svc.cluster.local:9092")
                .option("subscribe", "PMU-data")
                .load();

        Dataset<String> linesString = lines.as(Encoders.STRING());
        Dataset<PowerBean> powerBeanDataset = linesString.map(new MapFunction<String, PowerBean>() {
            @Override
            public PowerBean call(String s) throws Exception {
                SourceBean sourceBean = JSON.parseObject(s, SourceBean.class);
                return MathUtils.sourceToPower(sourceBean);
            }
        }, Encoders.bean(PowerBean.class));

        powerBeanDataset.writeStream().foreach(new InfluxDBSink()).start().awaitTermination();

    }
}
