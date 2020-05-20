package qianfengedu.demo05;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * @ClassName: KafkaSourceV2
 * @Description:  KafkaSource
 * @Author: Jokey Zhou
 * @Date: 2020/5/20
 * @赛博世界并不是辽阔的荒野，数据也不全是冰冷的记录，它是亲人的笑靥，它是我们的记忆。
 */
public class KafkaSourceV2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 开启checkpoint
        env.enableCheckpointing(5000);
        // 设置statebackend
        env.setStateBackend(new FsStateBackend(""));
        // 取消任务不删除checkpoint目录
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        // 如果和Kafka整合，一定要设置mode
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);


        Properties props = new Properties();
        // 指定kafka的broker地址
        props.setProperty("bootstrap.servers", "");
        // 指定组ID
        props.setProperty("group.id", "");
        // 如果没有记录偏移量，第一次从最开始消费
        props.setProperty("auto.offset.reset", "earliest");
        // 不自动提交偏移量，手动管理
        props.setProperty("enable.auto.commit", "false");

        // kafka Source
        FlinkKafkaConsumer kafkaConsumer = new FlinkKafkaConsumer<>(
                "",
                new SimpleStringSchema(),
                props
        );

        DataStreamSource source = env.addSource(kafkaConsumer);

        source.print();

        env.execute("KafkaSourceV2");
    }
}
