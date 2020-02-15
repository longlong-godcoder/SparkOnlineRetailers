import jdbc.JDBCHelper;

import java.sql.ResultSet;

public class TestJDBC {

    public void testQuery(){
        JDBCHelper instance = JDBCHelper.getInstance();
        String sql = "select id, name from test where id=?";
        instance.executeQuery(sql, new Object[]{1}, new JDBCHelper.QueryCallBack() {
            @Override
            public void process(ResultSet rs) throws Exception {
                while (rs.next()){
                    int id = rs.getInt(1);
                    String name = rs.getString(2);
                    System.out.println("id :" + id + " name: " + name);
                }
            }
        });
    }
}
