/**
 * Created by lancerlin on 2018/6/21.
 */

import java.sql.*;

public class PhoenixDemo2 {
    private Connection conn;
    private Statement sm;

    private static String driver = "org.apache.phoenix.jdbc.PhoenixDriver";
    private static String url = "jdbc:phoenix:1.hadoopdev.com,2.hadoopdev.com,3.hadoopdev.com:2181";

    // 用于初始化连接
    public void parpare() {
        try {
            try {
                Class.forName(driver);
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }
            Statement stmt = null;
            ResultSet rs = null;

            conn = DriverManager.getConnection(url);
            System.out.println("conn: " + conn);
            sm = conn.createStatement();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    // 表格创建
    public void createTable() {
        try {
            sm.executeUpdate("create table p_student (s_id integer not null" + " primary key,s_name varchar,age integer)");
//            sm.executeUpdate("create table p_court (c_id integer not null " + "primary key,c_name varchar)");
//            sm.executeUpdate("create table p_student_court (s_id integer not null ,c_id " + "integer not null,score integer,constraint scfk" + "primary key(s_id,c_id))");
            conn.commit();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    // 插入数据
    public void update() {
        try {
//            sm.executeUpdate("upsert into p_student values(1,'张三',20)");
//            sm.executeUpdate("upsert into p_student values(2,'李四',30)");
//            sm.executeUpdate("upsert into p_student values(3,'王五',28)");

            String sql = "upsert into p_student values(?, ?, ?)";
            PreparedStatement preparedStatement = conn.prepareStatement(sql);
            preparedStatement.setInt(1, 4);
            preparedStatement.setString(2, "");
            preparedStatement.setInt(3, 99);
            preparedStatement.execute();

//            sm.executeUpdate("upsert into p_court values(1,'英语')");
//            sm.executeUpdate("upsert into p_court values(2,'语文')");
//            sm.executeUpdate("upsert into p_court values(3,'数学')");
//            sm.executeUpdate("upsert into p_court values(4,'历史')");

//            sm.executeUpdate("upsert into p_student_court values(1,2,80)");
//            sm.executeUpdate("upsert into p_student_court values(2,1,95)");
//            sm.executeUpdate("upsert into p_student_court values(3,2,68)");
//            sm.executeUpdate("upsert into p_student_court values(1,3,88)");
//            sm.executeUpdate("upsert into p_student_court values(2,2,78)");
//            sm.executeUpdate("upsert into p_student_court values(3,1,98)");
            conn.commit();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    // 查询数据
    public void selectAll() {
        try {
            ResultSet rs = sm.executeQuery("select * from  p_student where S_ID=1");
//            ResultSet rs1 = sm.executeQuery("select * from  p_court");
//            ResultSet rs2 = sm.executeQuery("select * from  p_student_court");

            while (rs.next()) {
                System.out.println(rs.getInt(1) + ","
                        + rs.getString(2)
                        + "," + rs.getInt(3));
            }
//            System.out.println("---------------我是华丽的分割线------");
//            while (rs1.next()) {
//                System.out.println(rs1.getInt(1) + "," + rs1.getString(2));
//
//            }
//            System.out.println("--------------我是华丽的分割线-------------");
//            while (rs2.next()) {
//                System.out.println(rs2.getInt(1) + "," + rs2.getInt(2)
//                        + "," + rs2.getInt(3));
//            }

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    // 多表关联
    public void Join() {
        try {

            ResultSet rs = sm.executeQuery(
                    "select a.s_name,c.c_name,b.score "
                            + " from p_student a "
                            + " inner join p_student_court b "
                            + " on a.s_id = b.s_id "
                            + " inner join  p_court c "
                            + " on b.c_id=c.c_id");
            while (rs.next()) {
                System.out.println(rs.getString(1) + ","
                        + rs.getString(2) + "," + rs.getInt(3));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    //删除数据
    public void delete() {
        try {

            sm.executeUpdate(" delete from  P_COURT where C_ID = 2");
            System.out.println("删除成功");

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    // 用于关闭资源
    public void end() {

        try {
            sm.close();
            conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        PhoenixDemo2 p = new PhoenixDemo2();
        p.parpare();
//        p.createTable();
         p.update();
        // p.selectAll() ;
        // p.Join();
//        p.delete();

        p.end();
    }
}
