package utils;

import bean.PowerBean;
import org.apache.spark.sql.ForeachWriter;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Point;

import java.util.concurrent.TimeUnit;

public class InfluxDBSink extends ForeachWriter<PowerBean> {
    private InfluxDB connect = null;
    private String dataBaseName = "PMU_Power";
    private String dbURL = "http://10.66.101.230:30268";

    @Override
    public boolean open(long partitionId, long epochId) {
        connect = InfluxDBFactory.connect(dbURL, "root", "root");
        connect.enableBatch(500, 100, TimeUnit.MILLISECONDS);
        return true;
    }

    @Override
    public void process(PowerBean value) {
        Point.Builder builder = Point.measurement("PMUStream")
                .time(System.currentTimeMillis(), TimeUnit.MILLISECONDS)
                .tag("PMUID", Integer.toString(value.PMU_ID))
                .addField("Latency", System.currentTimeMillis() - value.Timestamp)
                .addField("A_Active", (float) value.ActPower_A)
                .addField("B_Active", (float) value.ActPower_B)
                .addField("C_Active", (int) value.ActPower_C)
                .addField("Sum_Active", (int) value.ActPower_Sum)
                .addField("A_Reactive", (int) value.ReaPower_A)
                .addField("B_Reactive", (int) value.ReaPower_B)
                .addField("C_Reactive", (int) value.ReaPower_C)
                .addField("Sum_Reactive", (int) value.ReaPower_Sum)
                .addField("A_Apparent", (int) value.AppPower_A)
                .addField("B_Apparent", (int) value.AppPower_B)
                .addField("C_Apparent", (int) value.AppPower_C)
                .addField("Sum_Apparent", (int) value.AppPower_Sum)
                .addField("PF", (float) value.PF);

        Point p = builder.build();
        connect.write(dataBaseName, "autogen", p);
    }

    @Override
    public void close(Throwable errorOrNull) {
        connect.close();
    }
}
