import static org.junit.jupiter.api.Assertions.*;

import java.sql.*;
import org.junit.jupiter.api.Test;

class MySqlJdbcTest {
  @Test
  void selfConnect() throws Exception {
    // Listen port 3307
    int port = 3307;
    // Preset database name
    String database = "demo_db";
    // Preset user name
    String user = "root";
    // Preset password
    String password = "4pdadmin";

    // Raise a connection to the server
    Class.forName("com.mysql.cj.jdbc.Driver");

    try (Connection conn =
            DriverManager.getConnection(
                "jdbc:mysql://127.0.0.1:" + port + "/" + database, user, password);
        // Query an arbitrary SQL
        PreparedStatement ps = conn.prepareStatement("select * from demo_table1");
        ResultSet rs = ps.executeQuery()) {

      while (rs.next()) {
        // c1 int, c2 int32, c3 smallint, c4 int16, c5 bigint, c6 int64, c7 float, c8 double, c9
        // timestamp, c10 date, c11 bool, c12 string, c13 varchar
        System.out.println("c1: " + rs.getInt("c1"));
        System.out.println("c2: " + rs.getInt("c2"));
        System.out.println("c3: " + rs.getInt("c3"));
        System.out.println("c4: " + rs.getInt("c4"));
        System.out.println("c5: " + rs.getLong("c5"));
        System.out.println("c6: " + rs.getLong("c6"));
        System.out.println("c7: " + rs.getFloat("c7"));
        System.out.println("c8: " + rs.getDouble("c8"));
        System.out.println("c9: " + rs.getTimestamp("c9"));
        System.out.println("c10: " + rs.getDate("c10"));
        System.out.println("c11: " + rs.getBoolean("c11"));
        System.out.println("c12: " + rs.getString("c12"));
        System.out.println("c13: " + rs.getString("c13"));
      }
    }
  }
}
