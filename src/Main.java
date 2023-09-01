import db.DataSource;

import java.sql.SQLException;

public class Main {
    public static void main(String[] args) throws SQLException {
        DataSource dataSource = new DataSource();
        boolean isOpen = dataSource.openConn();
        if (!isOpen) {
            System.out.println("Cannot open the connection");
        }
        dataSource.createTables();
        dataSource.execute("CREATE TABLE IF NOT EXISTS awards (award BOOLEAN NOT NULL CHECK (award IN (0,1)))");
        /*dataSource.execute("INSERT INTO movies VALUES(1, 'FlyBoys', 6.5)");
        dataSource.execute("INSERT INTO actors VALUES(1, 'James Franco', 1)");
        dataSource.execute("INSERT INTO genres VALUES(1, 1, 'Action')");
        dataSource.execute("INSERT INTO genres VALUES(1, 1, 'Drama')");*/
        System.out.println(dataSource.queryMovie("FlyBoys"));
        System.out.println(dataSource.queryActor("James Franco"));
        System.out.println(dataSource.queryGenre("Action"));
        System.out.println(dataSource.queryGenre("Drama"));
        dataSource.execute("CREATE TABLE IF NOT EXISTS awards (award BOOLEAN NOT NULL CHECK (award IN (0,1)))");
        //dataSource.dropTable("awards");

        if (isOpen) {
            dataSource.closeConn();
        }
    }
}
