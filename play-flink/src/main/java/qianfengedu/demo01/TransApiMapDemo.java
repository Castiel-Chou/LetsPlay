package qianfengedu.demo01;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @ClassName: TransApiMapDemo
 * @Description: Flink map() Function Test
 * @Author: Jokey Zhou
 * @Date: 2020/5/11 14:02
 * @赛博世界并不是辽阔的荒野，数据也不全是冰冷的记录，它是亲人的笑靥，它是我们的记忆。
 */
public class TransApiMapDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Integer> source = env.fromElements(1, 2, 3, 4, 5, 6);

        SingleOutputStreamOperator<Integer> map = source.map(new RichMapFunction<Integer, Integer>() {
            @Override
            public Integer map(Integer number) throws Exception {
                return number * 2;
            }
        });

        map.print();

        env.execute("TransApiMapDemo");
    }
}
