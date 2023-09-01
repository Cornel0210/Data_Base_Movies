package db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class DataSource {
    private static final String DB_NAME = "movies.db";
    private static final String CONNECTION_PATH = "jdbc:sqlite:E:\\05. java\\05. repositories\\Movies_DB\\src\\db\\" + DB_NAME;
    private Connection connection;

    public boolean openConn(){
        try {
            connection = DriverManager.getConnection(CONNECTION_PATH);
            return true;
        } catch (SQLException e){
            System.out.println(e.getMessage());
            e.printStackTrace();
            return false;
        }
    }

    public void closeConn(){
        try {
            if (connection != null) {
                connection.close();
            }
        } catch (SQLException e){
            System.out.println(e.getMessage());
            e.printStackTrace();
        }
    }
}
