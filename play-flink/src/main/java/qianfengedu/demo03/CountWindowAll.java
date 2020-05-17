package qianfengedu.demo03;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;

/**
 * @ClassName: CountWindowAll
 * @Description: 不分组将整体作为一个窗口
 * @Author: Jokey Zhou
 * @Date: 2020/5/14
 * @赛博世界并不是辽阔的荒野，数据也不全是冰冷的记录，它是亲人的笑靥，它是我们的记忆。
 */
public class CountWindowAll {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> source = env.socketTextStream("localhost", 8888);

        SingleOutputStreamOperator<Integer> map = source.map(new MapFunction<String, Integer>() {
            @Override
            public Integer map(String s) throws Exception {
                return Integer.parseInt(s);
            }
        });

        // 不分组，将整体当做一个组
        AllWindowedStream<Integer, GlobalWindow> windowAll = map.countWindowAll(5);

        // 在窗口中聚合
        SingleOutputStreamOperator<Integer> sum = windowAll.sum(0);

        sum.print();

        env.execute("CountWindowAll");
    }
}
