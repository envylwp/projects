/**
 * Created by lancerlin on 2018/6/21.
 */

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class PhoenixDemo1 {
    private static String driver = "org.apache.phoenix.jdbc.PhoenixDriver";
    private static String url = "jdbc:phoenix:1.hadoopdev.com,2.hadoopdev.com,3.hadoopdev.com:2181";

    public static void main(String[] args) throws SQLException {
        try {
            Class.forName(driver);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        Statement stmt = null;
        ResultSet rs = null;

        Connection con = DriverManager.getConnection(url);
        System.out.println(con);
        stmt = con.createStatement();
        String sql = "select * from test";
        rs = stmt.executeQuery(sql);
        while (rs.next()) {
            System.out.print("id:"+rs.getString("id"));
            System.out.println(",name:"+rs.getString("name"));
        }
        stmt.close();
        con.close();
    }
}
