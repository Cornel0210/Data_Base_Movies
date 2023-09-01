import db.DataSource;
import db.Movie;

import java.sql.SQLException;
import java.util.List;

public class Main {
    public static void main(String[] args) throws SQLException {
        DataSource dataSource = new DataSource();
        boolean isOpen = dataSource.openConn();
        if (!isOpen) {
            System.out.println("Cannot open the connection");
        }
        dataSource.dropTable("actors");
        dataSource.dropTable("movies");
        dataSource.dropTable("genres");
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
        //System.out.println(dataSource.insertIntoGenres(1, "Drama"));

        dataSource.insert("Todd Boyce", "FlyBoys", "Drama", 5.0d);
        dataSource.insert("Oscar Isaac", "Ex-Machina", "Drama", 7.7d);
        dataSource.insert("Oscar Isaac", "A Most Violent Year", "Action", 7.0d);
        dataSource.insert("Jessica Chastain", "A Most Violent Year", "Crime", 7.0d);
        dataSource.insert("Timothée Chalamet", "Dune", "Action", 7.0d);
        dataSource.insert("Rebecca Ferguson", "Dune", "Adventure", 7.0d);
        dataSource.insert("Zendaya", "Dune", "Drama", 7.0d);
        dataSource.insert("Oscar Isaac", "Dune", "Sci-Fi", 7.0d);
        dataSource.insert("Jason Momoa", "Dune", "Sci-Fi", 7.0d);

        System.out.println(dataSource.queryGenresForAMovie(1));
        System.out.println(dataSource.queryGenresForAMovie(2));
        System.out.println(dataSource.queryGenresForAMovie(3));
        System.out.println(dataSource.queryGenresForAMovie(4));

        System.out.println("------------------------------------");
        List<Movie> movies = dataSource.queryMovies();
        if (movies.isEmpty()){
            System.out.println("There are no movies in the DB.");
        } else {
            for (Movie movie : movies){
                System.out.printf("%03d - %20s - %.2f\n", movie.get_id(), movie.getName(), movie.getRating());
            }
        }


        //dataSource.execute("CREATE TABLE IF NOT EXISTS awards (award BOOLEAN NOT NULL CHECK (award IN (0,1)))");
        //dataSource.dropTable("awards");

        if (isOpen) {
            dataSource.closeConn();
        }
    }
}
