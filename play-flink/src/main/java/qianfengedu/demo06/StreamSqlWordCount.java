package qianfengedu.demo06;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;

/**
 * @ClassName: StreamSqlWordCount
 * @Description:  流式flink sql
 * @Author: Jokey Zhou
 * @Date: 2020/5/23
 * @赛博世界并不是辽阔的荒野，数据也不全是冰冷的记录，它是亲人的笑靥，它是我们的记忆。
 */
public class StreamSqlWordCount {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        DataStreamSource<String> source = env.socketTextStream("localhost", 8888);

        SingleOutputStreamOperator<String> flatmap = source.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                Arrays.stream(value.split(" ")).forEach(out::collect);
            }
        });

        // 注册Table
        tEnv.registerDataStream("t_wordcount", flatmap, "word");

        // 编写SQL
        Table table = tEnv.sqlQuery("SELECT word, count(1) counts FROM t_wordcount GROUP BY word");

        // 显示结果
        DataStream<Tuple2<Boolean, WordCount>> retractStream = tEnv.toRetractStream(table, WordCount.class);

        // 过滤累加之后的数据
        SingleOutputStreamOperator<Tuple2<Boolean, WordCount>> filter = retractStream.filter(new FilterFunction<Tuple2<Boolean, WordCount>>() {
            @Override
            public boolean filter(Tuple2<Boolean, WordCount> value) throws Exception {
                return value.f0;
            }
        });

        filter.print();

        env.execute("StreamSqlWordCount");
    }
}
