package test.flink.customer.datasink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

public class SinkToMySQL extends RichSinkFunction<Config> {
    PreparedStatement ps;
    private Connection connection;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        connection = getConnection();
        String sql = "insert into t_config(`label`, `remark`, `VALUE`, `voice_value`) values(?, ?, ?, ?);";
        ps = this.connection.prepareStatement(sql);
    }

    @Override
    public void invoke(Config value, Context context) throws Exception {
        //组装数据，执行插入操作
        ps.setString(1, value.getLabel());
        ps.setString(2, value.getRemark());
        ps.setString(3, value.getValue());
        ps.setString(4, value.getVoiceValue());
        ps.executeUpdate();
    }

    @Override
    public void close() throws Exception {
        super.close();
        //关闭连接和释放资源
        if (connection != null) {
            connection.close();
        }
        if (ps != null) {
            ps.close();
        }
    }
    private static Connection getConnection() {
        Connection con = null;
        try {
            Class.forName("com.mysql.jdbc.Driver");
            con = DriverManager.getConnection("jdbc:mysql://fddtestapidb.mysql.rds.aliyuncs.com:3306/yq_online?user=user_dev_ext&password=song@lsm.ging34&useUnicode=true&characterEncoding=UTF-8",
                    "user_dev_ext", "song@lsm.ging34");
        } catch (Exception e) {
            System.out.println("-----------mysql get connection has exception , msg = "+ e.getMessage());
        }
        return con;
    }
}
