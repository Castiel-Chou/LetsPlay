package qianfengedu.demo05;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.util.Properties;

/**
 * @ClassName: MapWithState
 * @Description: 使用keyedState实现类似sum的功能
 * @Author: Jokey Zhou
 * @Date: 2020/5/18
 * @赛博世界并不是辽阔的荒野，数据也不全是冰冷的记录，它是亲人的笑靥，它是我们的记忆。
 */
public class MapWithState {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.enableCheckpointing(5000);

        // 为了实现EXACTLY_ONCE，必须记录偏移量
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // 程序停止后不清除checkpoint目录
        env.getCheckpointConfig().enableExternalizedCheckpoints(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        Properties props = new Properties();
        // 指定kafka的broker地址
        props.setProperty("bootstrap.servers", "");
        // 指定组ID
        props.setProperty("group.id", "");
        // 如果没有记录偏移量，第一次从最开始消费
        props.setProperty("auto.offset.reset", "earliest");
        // kafka的消费者不自动提交偏移量，而是交给Flink通过checkpoint管理偏移量
        props.setProperty("enable.auto.commit", "false");

        // kafka Source
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                args[0],
                new SimpleStringSchema(),
                props
        );

        DataStreamSource<String> source = env.addSource(kafkaConsumer);

        SingleOutputStreamOperator<String> flatMap = source.flatMap(new FlatMapFunction<String, String>() {

            @Override
            public void flatMap(String line, Collector<String> out) throws Exception {
                String[] words = line.split(" ");
                for (String s : words) {
                    out.collect(s);
                }
            }
        });

        SingleOutputStreamOperator<Tuple2<String, Integer>> map = flatMap.map(new MapFunction<String, Tuple2<String, Integer>>() {

            @Override
            public Tuple2<String, Integer> map(String word) throws Exception {
                return Tuple2.of(word, 1);
            }
        });

        KeyedStream<Tuple2<String, Integer>, Tuple> keyedStream = map.keyBy(0);

        // 为了保证程序出现问题可以继续累加，要记录分组聚合的中间结果
        SingleOutputStreamOperator<Tuple2<String, Integer>> aggValue = keyedStream.map(new RichMapFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>() {

            private transient ValueState<Tuple2<String, Integer>> valueState;

            @Override
            public void open(Configuration parameters) throws Exception {
                // 初始化状态或恢复历史状态
                // 定义一个状态描述器
                ValueStateDescriptor<Tuple2<String, Integer>> descriptor =
                        new ValueStateDescriptor("wc-keyed-state",
                                TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {
                                }));

                valueState = getRuntimeContext().getState(descriptor);
            }

            @Override
            public Tuple2<String, Integer> map(Tuple2<String, Integer> value) throws Exception {

                String word = value.f0;
                Integer count = value.f1;
                // 获取状态
                Tuple2<String, Integer> historyKV = valueState.value();
                if (historyKV != null) {
                    historyKV.f1 += value.f1;
                    valueState.update(historyKV);
                    return historyKV;
                } else {
                    valueState.update(value);
                    return value;
                }
            }
        });

        aggValue.print();

        env.execute("MapWithState");

    }
}
