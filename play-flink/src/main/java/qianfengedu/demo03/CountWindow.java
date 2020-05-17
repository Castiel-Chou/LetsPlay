package qianfengedu.demo03;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;

/**
 * @ClassName: CountWindow
 * @Description: 分组聚合时间窗口
 *
 * 输入数据如下形式
 * spark,2
 * hadoop,3
 * ...
 *
 * @Author: Jokey Zhou
 * @Date: 2020/5/14
 * @赛博世界并不是辽阔的荒野，数据也不全是冰冷的记录，它是亲人的笑靥，它是我们的记忆。
 */
public class CountWindow {
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

        // 先分组，再划分窗口
        // 返回结果分别是输入数据，指定的分组字段，窗口信息
        WindowedStream<Tuple2<String, Integer>, Tuple, GlobalWindow> windowedStream =
                map.keyBy(0)
                .countWindow(5);  // 分组窗口内的数据累积到5条才会触发计算

        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = windowedStream.sum(1);

        sum.print();

        env.execute("CountWindow");
    }
}
