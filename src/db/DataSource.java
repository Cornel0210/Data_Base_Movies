package db;

import java.sql.*;

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
    private PreparedStatement insertIntoMovies;
    private PreparedStatement insertIntoActors;
    private PreparedStatement insertIntoGenres;

    private PreparedStatement queryMovie;
    private PreparedStatement queryActor;
    private PreparedStatement queryGenre;

    public boolean openConn(){
        try {
            connection = DriverManager.getConnection(CONNECTION_PATH);
            queryMovie = connection.prepareStatement(QUERY_MOVIE_BY_NAME);
            queryActor = connection.prepareStatement(QUERY_ACTOR_BY_NAME);
            queryGenre = connection.prepareStatement(QUERY_GENRE);
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
            if (queryActor(name) > 0) {
                return -1;
            }
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
            if (queryGenre(genre) > 0) {
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

    /*public void insert(String actor, String movie, String genre, double rating){
        if (actor == null || movie == null || genre == null || rating < 1.0d || rating >10.0d){
            System.out.println("Couldn't make the insertion.");
        }
        if (queryMovie(movie) < 1){
            insertIntoMovies()
        }
    }*/
}
