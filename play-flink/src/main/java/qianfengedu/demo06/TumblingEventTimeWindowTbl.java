package qianfengedu.demo06;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @ClassName: TumblingEventTimeWindowTbl
 * @Description:  滚动窗口的flink sql
 * @Author: Jokey Zhou
 * @Date: 2020/5/23
 * @赛博世界并不是辽阔的荒野，数据也不全是冰冷的记录，它是亲人的笑靥，它是我们的记忆。
 */
public class TumblingEventTimeWindowTbl {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        /*
        1000,u1,p1,5
        2000,u1,p1,5
        2000,u2,p1,3
        3000,u1,p1,5
        9999,u2,p1,3
        19999,u1,p1,5
         */
        DataStreamSource<String> source = env.socketTextStream("localhost", 8888);

        SingleOutputStreamOperator<Row> streamOperator = source.map(new MapFunction<String, Row>() {
            @Override
            public Row map(String line) throws Exception {
                String[] field = line.split(",");
                long time = Long.parseLong(field[0]);
                String uid = field[1];
                String pid = field[2];
                double money = Double.parseDouble(field[3]);

                return Row.of(time, uid, pid, money);
            }
        }).returns(Types.ROW(Types.LONG, Types.STRING, Types.STRING, Types.DOUBLE));


        SingleOutputStreamOperator<Row> timestampsAndWatermarks = streamOperator.assignTimestampsAndWatermarks(
                new BoundedOutOfOrdernessTimestampExtractor<Row>(Time.seconds(0)) {
                    @Override
                    public long extractTimestamp(Row element) {
                        return (long) element.getField(0);
                    }
                }
        );

        tEnv.registerDataStream("t_order", timestampsAndWatermarks,
                "atime, uid, pid, money, rowtime.rowtime");

        Table table = tEnv.scan("t_order")
                .window(Tumble.over("10.seconds").on("rowtime").as("win"))
                .groupBy("uid, win")
                .select("uid, win.start, win.end, win.rowtime, money.sumas total");

        tEnv.toAppendStream(table, Row.class).print();

        env.execute("TumblingEventTimeWindowTbl");
    }
}
