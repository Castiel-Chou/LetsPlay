package qianfengedu.project01;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * @ClassName: Data2ActivityBeanFunc
 * @Description: 将日志处理的过程抽成一个类
 * @Author: Jokey Zhou
 * @Date: 2020/5/17
 * @赛博世界并不是辽阔的荒野，数据也不全是冰冷的记录，它是亲人的笑靥，它是我们的记忆。
 */
public class Data2ActivityBeanFunc extends RichMapFunction<String, ActivityBean> {

    private Connection conn;

    @Override
    public void open(Configuration parameters) throws Exception {
        conn = DriverManager.getConnection("jdbc:mysql://10.101.12.14:3306/?characterEncoding=UTF-8",
                "root", "1q2w3e4r");
    }

    @Override
    public void close() throws Exception {
        super.close();
        conn.close();
    }

    @Override
    public ActivityBean map(String line) throws Exception {
        String[] field = line.split(",");
        String uid = field[0];

        String aid = field[1];
        String name = null;
        // 需要根据aid，从MySQL查询出对应的name
        PreparedStatement preparedStatement = conn.prepareStatement("SELECT name " +
                "FROM test_data.t_activity t " +
                "WHERE t.name = ?");

        preparedStatement.setString(1, aid);
        ResultSet resultSet = preparedStatement.executeQuery();
        while (resultSet.next()) {
            name = resultSet.getString(1);
        }
        preparedStatement.close();



        String time = field[2];
        Integer eventType = Integer.parseInt(field[3]);
        String province = field[4];

        return ActivityBean.of(uid, aid, name, time, eventType, province);
    }
}
