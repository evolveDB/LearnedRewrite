package org.db.autonomous;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

public class DBConn {
  private String host;
  private String port;
  private String user;
  private String password;
  public String dbname;
  private Connection conn;
  private String DBDriver;

  public DBConn(String host, String port, String user, String password, String dbname, String DBDriver)
          throws Exception {
    this.host = host;
    this.port = port;
    this.user = user;
    this.password = password;
    this.dbname = dbname;
    this.DBDriver = DBDriver;
    get_conn();
  }

  private void get_conn() throws Exception{
    //Class.forName(this.DBDriver);
    Class.forName(this.DBDriver);

    // String url, String user, String password
    //this.conn = DriverManager.getConnection("jdbc:mysql://166.111.121.62:3306/imdbload", user="feng", password="db10204");
    this.conn = DriverManager.getConnection("jdbc:mysql://" + host + ":" + port + "/" + dbname, user, password);
  }

  public ArrayList getTableName() {
    ArrayList list = new ArrayList();
    try {
      Statement stmt = this.conn.createStatement();
      stmt.setQueryTimeout(5);
      boolean success = stmt.execute("SELECT tablename FROM pg_tables WHERE tablename NOT LIKE 'pg%' AND tablename NOT LIKE 'sql_%' ORDER BY tablename;");

      // System.out.println("getTableName success:" + success);
      if (success){
        ResultSet res = stmt.getResultSet();
        while(res.next()){
          String s = res.getString(1);
          list.add(s);
        }
        System.out.println("获取table成功: " + list);
        stmt.close();
      } else {
        System.out.println("获取table name失败");
      }
    } catch (SQLException e) {
      System.out.println("获取table name失败：" + e);
    }
    return list;
  }


  public float getCost(String sql) {
//    todo 处理请求异常
    float cost = -1;
    try {
      Statement stmt = this.conn.createStatement();
      // System.out.println("getting cost for sql:");
//      System.out.println(sql);
      stmt.setQueryTimeout(5);
      boolean success = stmt.execute("explain FORMAT=JSON " + sql);
      if (success){
        ResultSet res = stmt.getResultSet();

        ResultSet rs = stmt.getResultSet();
        rs.next();
        String s = rs.getString(1);

        // System.out.println(s);

        //String s = res.getArray(1).toString().strip();
        JSONObject jobject = (JSONObject) JSONObject.parse(s);
        //JSONObject jobject = (JSONObject) jarray.get(0);
        jobject = (JSONObject) jobject.get("query_block");
        jobject = (JSONObject) jobject.get("cost_info");

        if (jobject != null) {
          String s_cost;
          s_cost = (String) jobject.get("query_cost"); // .floatValue()
          cost = Float.parseFloat(s_cost);
        }

        stmt.close();
      } else {
        System.out.println("获取Cost失败：" + sql);
      }
    } catch (SQLException e) {
      e.printStackTrace();
      cost = -1;
    }
    return cost;
  }
}
