package qianfengedu.flink_utils;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * @ClassName: UerDefinedFlinkUtils
 * @Description: 自定义的一些处理方法
 * @Author: Jokey Zhou
 * @Date: 2020/5/17
 * @赛博世界并不是辽阔的荒野，数据也不全是冰冷的记录，它是亲人的笑靥，它是我们的记忆。
 */
public class UserDefinedFlinkUtils {

    private static final StreamExecutionEnvironment env =
            StreamExecutionEnvironment.getExecutionEnvironment();

    /**
     *
     * @param args
     * @param simpleStringSchema
     * @return
     */
    public static DataStreamSource<String> createKafkaSource
            (String[] args, SimpleStringSchema simpleStringSchema) {
        String topic = args[0];
        String groupId = args[1];
        String borkerList = args[2];

        Properties props = new Properties();
        // 指定kafka的broker地址
        props.setProperty("bootstrap.servers", borkerList);
        // 指定组ID
        props.setProperty("group.id", groupId);
        // 如果没有记录偏移量，第一次从最开始消费
        props.setProperty("auto.offset.reset", "earliest");

        // kafka Source
        FlinkKafkaConsumer kafkaConsumer = new FlinkKafkaConsumer<>(
                topic,
                new SimpleStringSchema(),
                props
        );

        return env.addSource(kafkaConsumer);

    }

    public static StreamExecutionEnvironment getEnv() {
        return env;
    }
}
