package db;

import java.sql.*;
import java.util.LinkedList;
import java.util.List;

public class DataSource {
    private static final String DB_NAME = "movies.db";
    private static final String CONNECTION_PATH = "jdbc:sqlite:E:\\05. java\\05. repositories\\Movies_DB\\src\\db\\" + DB_NAME;
    private Connection connection;

    public static final String TABLE_MOVIES = "movies";
    public static final String TABLE_ACTORS = "actors";
    public static final String TABLE_GENRES = "genres";

    public static final String COLUMN_MOVIE_ID = "_id";
    public static final String COLUMN_MOVIE_NAME = "name";
    public static final String COLUMN_MOVIE_RATING = "rating";

    public static final String COLUMN_ACTOR_ID = "_id";
    public static final String COLUMN_ACTOR_NAME = "name";
    public static final String COLUMN_ACTOR_MOVIE = "movie";

    public static final String COLUMN_GENRE_ID = "_id";
    public static final String COLUMN_GENRE_NAME = "genre";
    public static final String COLUMN_GENRE_MOVIE = "movie";

    public static final String INSERT_INTO_MOVIES = "INSERT INTO " + TABLE_MOVIES +
            " (" + COLUMN_MOVIE_NAME + ", " + COLUMN_MOVIE_RATING + ") VALUES(?, ?)";
    public static final String INSERT_INTO_ACTORS = "INSERT INTO " + TABLE_ACTORS +
            " (" + COLUMN_ACTOR_NAME + ", " + COLUMN_ACTOR_MOVIE + ") VALUES(?, ?)";
    public static final String INSERT_INTO_GENRES = "INSERT INTO " + TABLE_GENRES +
            " (" + COLUMN_GENRE_MOVIE + ", " + COLUMN_GENRE_NAME + ") VALUES(?, ?)";

    public static final String QUERY_MOVIE_BY_NAME = "SELECT " + COLUMN_MOVIE_ID + " FROM " + TABLE_MOVIES +
            " WHERE " + COLUMN_MOVIE_NAME + " = ? COLLATE NOCASE";
    public static final String QUERY_ACTOR_BY_NAME = "SELECT " + COLUMN_ACTOR_ID + " FROM " + TABLE_ACTORS +
            " WHERE " + COLUMN_ACTOR_NAME + " = ? ";
    public static final String QUERY_GENRE = "SELECT " + COLUMN_GENRE_ID + " FROM " + TABLE_GENRES +
            " WHERE " + COLUMN_GENRE_NAME + " = ? ";

    public static final String QUERY_GENRES_FOR_A_MOVIE = "SELECT " + COLUMN_GENRE_NAME + " FROM " + TABLE_GENRES +
            " WHERE " + COLUMN_GENRE_MOVIE + " = ? ";
    public static final String QUERY_MOVIES_FOR_AN_ACTOR = "SELECT " + COLUMN_ACTOR_MOVIE + " FROM " + TABLE_ACTORS +
            " WHERE " + COLUMN_ACTOR_NAME + " = ?";
    public static final String QUERY_ACTOR_MOVIE = "SELECT " + COLUMN_ACTOR_ID + " FROM " + TABLE_ACTORS +
            " WHERE " + COLUMN_ACTOR_NAME + " = ? AND " + COLUMN_ACTOR_MOVIE + " = ?";
    public static final String QUERY_MOVIES = "SELECT * FROM " + TABLE_MOVIES;
    private PreparedStatement insertIntoMovies;
    private PreparedStatement insertIntoActors;
    private PreparedStatement insertIntoGenres;

    private PreparedStatement queryMovie;
    private PreparedStatement queryActor;
    private PreparedStatement queryGenre;
    private PreparedStatement queryGenresForAMovie;
    private PreparedStatement queryMoviesForAnActor;
    private PreparedStatement queryActorMovie;

    public boolean openConn(){
        try {
            connection = DriverManager.getConnection(CONNECTION_PATH);
            queryMovie = connection.prepareStatement(QUERY_MOVIE_BY_NAME);
            queryActor = connection.prepareStatement(QUERY_ACTOR_BY_NAME);
            queryGenre = connection.prepareStatement(QUERY_GENRE);
            queryMoviesForAnActor = connection.prepareStatement(QUERY_MOVIES_FOR_AN_ACTOR);
            queryGenresForAMovie = connection.prepareStatement(QUERY_GENRES_FOR_A_MOVIE);
            queryActorMovie = connection.prepareStatement(QUERY_ACTOR_MOVIE);
            insertIntoMovies = connection.prepareStatement(INSERT_INTO_MOVIES, Statement.RETURN_GENERATED_KEYS);
            insertIntoActors = connection.prepareStatement(INSERT_INTO_ACTORS, Statement.RETURN_GENERATED_KEYS);
            insertIntoGenres = connection.prepareStatement(INSERT_INTO_GENRES, Statement.RETURN_GENERATED_KEYS);
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

    public void createTables (){
        Statement statement;
        try {
            statement = connection.createStatement();
            statement.execute("CREATE TABLE IF NOT EXISTS " + TABLE_ACTORS +
                    " (" + COLUMN_ACTOR_ID + " INTEGER PRIMARY KEY, " + COLUMN_ACTOR_NAME + " TEXT NOT NULL, " +
                    COLUMN_ACTOR_MOVIE + " INTEGER NOT NULL)");
            statement.execute("CREATE TABLE IF NOT EXISTS " + TABLE_MOVIES +
                    "(" + COLUMN_MOVIE_ID + " INTEGER PRIMARY KEY, " + COLUMN_MOVIE_NAME + " TEXT NOT NULL, " +
                    COLUMN_MOVIE_RATING + " REAL)");
            statement.execute("CREATE TABLE IF NOT EXISTS " + TABLE_GENRES +
                    "(" + COLUMN_GENRE_ID + " INTEGER PRIMARY KEY, " + COLUMN_GENRE_MOVIE + " INTEGER NOT NULL, " +
                    COLUMN_GENRE_NAME + " TEXT NOT NULL)");
        } catch (SQLException e){
            System.out.println("createTables error: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    public void createTable (String input){
        String[] data = input.trim().split(",");
        StringBuilder sb = new StringBuilder("create ");
        sb.append(data[0].trim()).append(" IF NOT EXISTS ").append(data[1].trim()).append(" (");
        for (int i = 2; i < data.length; i=i+2) {
            sb.append(data[i].trim()).append(" ").append(data[i+1].trim()).append(", ");
        }
        sb.replace(sb.length()-2, sb.length(), "");
        sb.append(")");
        System.out.println(sb);

        Statement statement;
        try {
            statement = connection.createStatement();
            statement.execute(sb.toString());
        } catch (SQLException e){
            System.out.println(e.getMessage());
            e.printStackTrace();
        }
    }
    public void execute(String input){
        if (input != null){
            Statement statement;
            try {
                statement = connection.createStatement();
                statement.execute(input);
            } catch (SQLException e){
                System.out.println(e.getMessage());
                e.printStackTrace();
            }
        }
    }

    public void dropTable(String tableName){
        if (tableName != null) {
            String command = "DROP TABLE IF EXISTS " + tableName;
            Statement statement;
            try {
                statement = connection.createStatement();
                statement.execute(command);
            } catch (SQLException e){
                System.out.println(e.getMessage());
                e.printStackTrace();
            }
        }
    }

    public int queryMovie(String name) throws SQLException{
        queryMovie.setString(1, name);
        ResultSet resultSet = queryMovie.executeQuery();
        if (resultSet.next()){
            return resultSet.getInt(1);
        } else return -1;
    }
    public List<Movie> queryMovies(){
        Statement statement;
        try {
            statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(QUERY_MOVIES);
            List<Movie> movies = new LinkedList<>();
            while (resultSet.next()){
                Movie movie = new Movie();
                movie.set_id(resultSet.getInt(1));
                movie.setName(resultSet.getString(2));
                movie.setRating(resultSet.getDouble(3));
                movies.add(movie);
            }
            return movies;
        } catch (SQLException e){
            System.out.println("QueryMovies: " + e.getMessage());
            e.printStackTrace();
            return null;
        }
    }

    public int queryActor(String name) throws SQLException{
        queryActor.setString(1, name);
        ResultSet resultSet = queryActor.executeQuery();
        if (resultSet.next()){
            return resultSet.getInt(1);
        } else {
            return -1;
        }
    }

    public int queryGenre(String name) throws SQLException{
        queryGenre.setString(1, name);
        ResultSet resultSet = queryGenre.executeQuery();
        if (resultSet.next()){
            return resultSet.getInt(1);
        } else {
            return -1;
        }
    }

    public List<String> queryGenresForAMovie(int movie){
        if (movie < 1){
            return null;
        }
        try {
            queryGenresForAMovie.setInt(1, movie);
            ResultSet resultSet = queryGenresForAMovie.executeQuery();
            List<String> list = new LinkedList<>();
            while (resultSet.next()){
                list.add(resultSet.getString(1));
            }
            return list;
        } catch (SQLException e){
            System.out.println("queryGenresForAMovie: " + e.getMessage());
            e.printStackTrace();
            return null;
        }
    }

    public List<Integer> queryMoviesForAnActor(String name){
        if (name == null){
            return null;
        }
        try {
            queryMoviesForAnActor.setString(1, name);
            ResultSet resultSet = queryMoviesForAnActor.executeQuery();
            List<Integer> list = new LinkedList<>();
            while (resultSet.next()){
                list.add(resultSet.getInt(1));
            }
            return list;
        } catch (SQLException e){
            System.out.println("queryMoviesForAnActor: " + e.getMessage());
            e.printStackTrace();
            return null;
        }
    }

    public int queryActorMovie (String actor, String movie) throws  SQLException{
        if (actor == null || movie == null){
            return -1;
        }
        int movieId = queryMovie(movie);
        if (movieId < 1) {
            throw new SQLException("Movie not found");
        }
        queryActorMovie.setString(1, actor);
        queryActorMovie.setInt(2, movieId);
        ResultSet resultSet = queryActorMovie.executeQuery();
        if (resultSet.next()){
            return resultSet.getInt(1);
        } else {
            return -1;
        }
    }


    public int insertIntoMovies(String name, double rating){
        if (name == null || rating < 1.0d || rating > 10.0d){
            return -1;
        }
        try {
            if (queryMovie(name) > 0){
                return -1;
            }
            insertIntoMovies.setString(1, name);
            insertIntoMovies.setDouble(2, rating);
            int affectedRows = insertIntoMovies.executeUpdate();
            if (affectedRows != 1){
                throw new SQLException("Couldn't insert the movie.");
            }
            ResultSet resultSet = insertIntoMovies.getGeneratedKeys();
            if (resultSet.next()){
                return resultSet.getInt(1);
            } else {
                throw new SQLException("insertIntoMovies: Fatal Error.");
            }
        } catch (SQLException e){
            System.out.println("insertIntoMovies: " + e.getMessage());
            e.printStackTrace();
        }
        return -1;
    }
    public int insertIntoActors(String name, int movie){
        if ( name == null || movie < 1){
            return -1;
        }
        try {
            insertIntoActors.setString(1, name);
            insertIntoActors.setInt(2, movie);
            int affectedRows = insertIntoActors.executeUpdate();
            if (affectedRows != 1){
                throw new SQLException("Couldn't insert the actor.");
            }
            ResultSet resultSet = insertIntoMovies.getGeneratedKeys();
            if (resultSet.next()){
                return resultSet.getInt(1);
            } else {
                throw new SQLException("insertIntoActors: Fatal Error.");
            }
        } catch (SQLException e) {
            System.out.println("insertIntoActors: " + e.getMessage());
            e.printStackTrace();
        }
        return -1;
    }

    public int insertIntoGenres(int movie, String genre){
        if (movie < 1 || genre == null){
            return -1;
        }
        try {
            List<String> genres = queryGenresForAMovie(movie);
            if (genres.contains(genre)) {
                return -1;
            }
            insertIntoGenres.setInt(1, movie);
            insertIntoGenres.setString(2, genre);
            int affectedRows = insertIntoGenres.executeUpdate();
            if (affectedRows != 1){
                throw new SQLException("Couldn't insert the genre.");
            }
            ResultSet resultSet = insertIntoMovies.getGeneratedKeys();
            if (resultSet.next()){
                return resultSet.getInt(1);
            } else {
                throw new SQLException("insertIntoGenres: Fatal Error.");
            }
        } catch (SQLException e){
            System.out.println("insertIntoGenres: " + e.getMessage());
            e.printStackTrace();
        }
        return -1;
    }

    public void insert(String actor, String movie, String genre, double rating){
        if (actor == null || movie == null || genre == null || rating < 1.0d || rating >10.0d){
            System.out.println("Couldn't make the insertion.");
        }
        try {
            connection.setAutoCommit(false);

            int movieId = queryMovie(movie);
            if (movieId < 1){
                movieId = insertIntoMovies(movie, rating);
            }

            if (queryActorMovie(actor, movie) < 1){
                insertIntoActors(actor, movieId);
            }

           List<String> genres = queryGenresForAMovie(movieId);
            int genreId = -1;
            if (!genres.contains(genre)){
                genreId = insertIntoGenres(movieId, genre);
            } else {
                genreId = 1;
            }
            if (genreId > 0) {
                connection.commit();
            } else {
                throw new SQLException("Insertion failed.");
            }
        } catch (SQLException e){
            System.out.println("insert: " + e.getMessage());
            e.printStackTrace();

            try {
                System.out.println("Performing rollback.");
                connection.rollback();
            } catch (SQLException re){
                System.out.println("Performing rollback error: " + re.getMessage());
                re.printStackTrace();
            }
        } finally {
            try {
                connection.setAutoCommit(true);
            } catch (SQLException e){
                System.out.println("Couldn't set autoCommit to true.");
            }
        }
    }

    public List<MovieAllDetails> getMovieDetails(){
        String cmd = "SELECT " + TABLE_MOVIES + "." + COLUMN_MOVIE_NAME + " AS movie, " + TABLE_ACTORS + "." + COLUMN_ACTOR_NAME + " AS actor, " +
                TABLE_GENRES + "." + COLUMN_GENRE_NAME + ", " + TABLE_MOVIES + "." + COLUMN_MOVIE_RATING +
                " FROM " + TABLE_MOVIES + " JOIN " + TABLE_ACTORS + " ON " + TABLE_MOVIES + "." + COLUMN_MOVIE_ID + " = " +
                TABLE_ACTORS + "." + COLUMN_ACTOR_MOVIE +
                " JOIN " + TABLE_GENRES + " ON " + TABLE_GENRES + "." + COLUMN_GENRE_MOVIE + " = " + TABLE_MOVIES + "." + COLUMN_MOVIE_ID +
                " ORDER BY " + TABLE_MOVIES + "." + COLUMN_MOVIE_NAME;
        System.out.println(cmd);
        Statement statement;
        try {
            statement = connection.createStatement();
            statement.execute(cmd);
            ResultSet resultSet = statement.getResultSet();
            List<MovieAllDetails> list = new LinkedList<>();
            while (resultSet.next()){
                MovieAllDetails movie = new MovieAllDetails();
                movie.setMovie(resultSet.getString(1));
                movie.setActor(resultSet.getString(2));
                movie.setGenre(resultSet.getString(3));
                movie.setRating(resultSet.getDouble(4));
                list.add(movie);
            }
            return list;
        } catch (SQLException e){
            System.out.println(e.getMessage());
            e.printStackTrace();
            return null;
        }
    }
}
