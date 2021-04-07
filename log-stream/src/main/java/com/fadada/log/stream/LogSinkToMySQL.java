package com.fadada.log.stream;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

public class LogSinkToMySQL extends RichSinkFunction<LogWindowResult> {
    PreparedStatement ps;
    private Connection connection;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        connection = getConnection();
        String sql = "insert into t_log_stream(`app_name`, `logs`) values(?, ?);";
        ps = this.connection.prepareStatement(sql);
    }

    @Override
    public void invoke(LogWindowResult value, Context context) throws Exception {
        //组装数据，执行插入操作
        /*ps.setString(1, value.getAppName());
        ps.setString(2, JSONObject.toJSONString(value.getLogs()));
        ps.executeUpdate();*/
        System.out.println("sign 数据成功， size = " + value.getLogs().size());
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
            con = DriverManager.getConnection("jdbc:mysql://develop132.mysql.rds.aliyuncs.com:3306/fdd_api_v3?user=yq365&password=AccessLogValve991&useUnicode=true&characterEncoding=UTF-8",
                    "yq365", "AccessLogValve991");
        } catch (Exception e) {
            System.out.println("-----------mysql get connection has exception , msg = "+ e.getMessage());
        }
        return con;
    }
}
