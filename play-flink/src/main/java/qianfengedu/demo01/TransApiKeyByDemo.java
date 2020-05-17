package qianfengedu.demo01;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @ClassName: TransApiKeyByDemo
 * @Description: flink keyby算子的demo
 * @Author: Jokey Zhou
 * @Date: 2020/5/11 14:11
 * @赛博世界并不是辽阔的荒野，数据也不全是冰冷的记录，它是亲人的笑靥，它是我们的记忆。
 */
public class TransApiKeyByDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        /**
         * bigdata1,hadoop,1
         * bigdata1,flink,1
         * bigdata1,spark,1
         * bigdata1,hadoop,1
         * bigdata2,kafka,1
         * bigdata2,hive,1
         * bigdata2,hive,1
         * bigdata1,hive,1
         */
        DataStreamSource<String> source = env.socketTextStream("localhost", 8888);

        SingleOutputStreamOperator<Tuple3<String, String, Integer>> map = source.map(new RichMapFunction<String, Tuple3<String, String, Integer>>() {
            @Override
            public Tuple3<String, String, Integer> map(String line) throws Exception {
                String[] fields = line.split(",");
                String field1 = fields[0];
                String field2 = fields[1];
                Integer fields3 = Integer.parseInt(fields[2]);
                return Tuple3.of(field1, field2, fields3);
            }
        });

        KeyedStream<Tuple3<String, String, Integer>, Tuple> keyBy = map.keyBy(0, 1);
        SingleOutputStreamOperator<Tuple3<String, String, Integer>> sum = keyBy.sum(2);

        sum.print();

        env.execute("TransApiKeyByDemo");
    }
}
