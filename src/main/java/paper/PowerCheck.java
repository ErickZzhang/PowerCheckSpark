package paper;
import bean.PowerBean;
import bean.SourceBean;
import com.alibaba.fastjson.JSON;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.slf4j.Logger;
import scala.Function1;
import utils.InfluxDBSink;
import utils.MathUtils;

import java.util.Properties;

public class PowerCheck {
    public static void main(String[] args) throws Exception {

        SparkSession spark = SparkSession
                .builder()
                .appName("PowerCheckSpark")
                .getOrCreate();
        Logger log = spark.sparkContext().log();
//        Properties prop = new Properties();
//        prop.setProperty("source.topic", "PMU-data");
//        prop.setProperty("bootstrap.servers", "10.66.101.210:31090,10.66.101.210:31091,10.66.101.210:31092");
//        prop.setProperty("group.id", "demo1");
//        prop.setProperty("auto.offset.reset", "earliest");
//        prop.setProperty("enable.auto.submit","true");
//        prop.setProperty("jobName", "defaultJobName");
//        prop.setProperty("kafkaParallelism","3");

        Dataset<Row> lines = spark.readStream().format("kafka")
                .option("kafka.bootstrap.servers", "kafka-0.kafka-headless.default.svc.cluster.local:9092,kafka-1.kafka-headless.default.svc.cluster.local:9092,kafka-2.kafka-headless.default.svc.cluster.local:9092")
                .option("subscribe", "PMU-data")
                .option("startingOffsets", "latest")
                .load();

        Dataset<String> linesString = lines.selectExpr("CAST(value AS STRING)").as(Encoders.STRING());

        Dataset<PowerBean> powerBeanDataset = linesString.map(new MapFunction<String, PowerBean>() {
            @Override
            public PowerBean call(String s) throws Exception {
                log.info(s);

                SourceBean sourceBean = JSON.parseObject(s, SourceBean.class);
                return MathUtils.sourceToPower(sourceBean);
            }
        }, Encoders.bean(PowerBean.class));

        powerBeanDataset.writeStream().foreach(new InfluxDBSink()).start().awaitTermination();

    }
}
