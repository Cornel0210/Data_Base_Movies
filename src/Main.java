import db.DataSource;

import java.sql.SQLException;

public class Main {
    public static void main(String[] args) throws SQLException {
        DataSource dataSource = new DataSource();
        boolean isOpen = dataSource.openConn();
        if (!isOpen) {
            System.out.println("Cannot open the connection");
        }
        /*dataSource.dropTable("actors");
        dataSource.dropTable("movies");
        dataSource.dropTable("genres");*/
        dataSource.createTables();
        dataSource.execute("CREATE TABLE IF NOT EXISTS awards (award BOOLEAN NOT NULL CHECK (award IN (0,1)))");
        System.out.println(dataSource.queryMovie("FlyBoys"));
        System.out.println(dataSource.queryActor("James Franco"));
        System.out.println(dataSource.queryGenre("Action"));
        System.out.println(dataSource.queryGenre("Drama"));
        System.out.println(dataSource.insertIntoMovies("FlyBoys", 6.5d));
        System.out.println(dataSource.insertIntoActors("James Franco", 1));
        System.out.println(dataSource.insertIntoActors("Jennifer Decker", 1));
        System.out.println(dataSource.insertIntoActors("Jean Reno", 1));
        System.out.println(dataSource.insertIntoGenres(1, "Action"));
        System.out.println(dataSource.insertIntoGenres(1, "Drama"));

        dataSource.execute("CREATE TABLE IF NOT EXISTS awards (award BOOLEAN NOT NULL CHECK (award IN (0,1)))");
        //dataSource.dropTable("awards");

        if (isOpen) {
            dataSource.closeConn();
        }
    }
}
