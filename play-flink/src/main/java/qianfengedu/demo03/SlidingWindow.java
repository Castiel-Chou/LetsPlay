package qianfengedu.demo03;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * @ClassName: SlidingWindow
 * @Description:  滑动窗口Demo2
 * @Author: Jokey Zhou
 * @Date: 2020/5/16
 * @赛博世界并不是辽阔的荒野，数据也不全是冰冷的记录，它是亲人的笑靥，它是我们的记忆。
 */
public class SlidingWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> source = env.socketTextStream("localhost", 8888);

        SingleOutputStreamOperator<Tuple2<String, Integer>> map = source.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String s) throws Exception {
                String[] fields = s.split(",");
                String word = fields[0];
                Integer num = Integer.parseInt(fields[1]);
                return Tuple2.of(word, num);
            }
        });

        // 根据指定字段分组
        KeyedStream<Tuple2<String, Integer>, Tuple> keyedStream = map.keyBy(0);

        WindowedStream<Tuple2<String, Integer>, Tuple, TimeWindow> windowedStream =
                keyedStream.timeWindow(Time.seconds(10), Time.seconds(5));

        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = windowedStream.sum(1);

        sum.print();

        env.execute("SlidingWindow");
    }
}
