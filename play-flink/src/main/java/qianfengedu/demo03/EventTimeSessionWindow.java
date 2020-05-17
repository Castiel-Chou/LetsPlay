package qianfengedu.demo03;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * @ClassName: EventTimeSessionWindow
 * @Description:  时间产生时间窗口处理Demo
 * @Author: Jokey Zhou
 * @Date: 2020/5/16
 * @赛博世界并不是辽阔的荒野，数据也不全是冰冷的记录，它是亲人的笑靥，它是我们的记忆。
 */
public class EventTimeSessionWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 告诉程序需要使用EventTime作为时间处理标准
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        /*
        输入的数据形式为：
        1575017261000,hadoop,1
        1575017262000,spark,1
        1675017280000,flink,1
         */
        DataStreamSource<String> source = env.socketTextStream("localhost", 8888);

        // 提取时间字段
        // 请注意 这个曹组仅仅是提取时间字段，并不会改变输入数据的样子
        SingleOutputStreamOperator<String> stringSingleOutputStreamOperator =
                source.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<String>(Time.seconds(0)) {
            @Override
            public long extractTimestamp(String element) {
                String[] fields = element.split(",");
                return Long.parseLong(fields[0]);
            }
        });

        SingleOutputStreamOperator<Tuple2<String, Integer>> map = stringSingleOutputStreamOperator
                .map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String s) throws Exception {
                String[] fields = s.split(",");
                String word = fields[1];
                Integer num = Integer.parseInt(fields[2]);
                return Tuple2.of(word, num);
            }
        });

        // 根据指定字段分组
        KeyedStream<Tuple2<String, Integer>, Tuple> keyedStream = map.keyBy(0);

        // 数据携带的Event时间相差5秒就会触发窗口
        WindowedStream<Tuple2<String, Integer>, Tuple, TimeWindow> window =
                keyedStream.window(EventTimeSessionWindows.withGap(Time.seconds(5)));

        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = window.sum(1);

        sum.print();

        env.execute("EventTimeSessionWindow");
    }
}
