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

    public static final String INSERT_INTO_MOVIES = "INSERT INTO " + TABLE_MOVIES + " VALUES(?, ?, ?)";
    public static final String INSERT_INTO_ACTORS = "INSERT INTO " + TABLE_ACTORS + " VALUES(?, ?, ?)";
    public static final String INSERT_INTO_GENRES = "INSERT INTO " + TABLE_GENRES + " VALUES(?, ?, ?)";

    public static final String QUERY_MOVIE_BY_NAME = "SELECT " + COLUMN_MOVIE_ID + " FROM " + TABLE_MOVIES +
            " WHERE " + COLUMN_MOVIE_NAME + " = ? ";
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
                    " (" + COLUMN_ACTOR_ID + " INTEGER NOT NULL, " + COLUMN_ACTOR_NAME + " TEXT NOT NULL, " +
                    COLUMN_ACTOR_MOVIE + " INTEGER NOT NULL)");
            statement.execute("CREATE TABLE IF NOT EXISTS " + TABLE_MOVIES +
                    "(" + COLUMN_MOVIE_ID + " INTEGER NOT NULL, " + COLUMN_MOVIE_NAME + " TEXT NOT NULL, " +
                    COLUMN_MOVIE_RATING + " REAL)");
            statement.execute("CREATE TABLE IF NOT EXISTS " + TABLE_GENRES +
                    "(" + COLUMN_GENRE_ID + " INTEGER NOT NULL, " + COLUMN_GENRE_MOVIE + " INTEGER NOT NULL, " +
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


   /* public boolean insertIntoMovies(int id, String name, double rating){
        if (id < 1 || name == null || rating < 1.0d || rating > 5.0d){
            return false;
        }
        try {

        }
    }
    public boolean insertIntoActors(int id, String name, int movie){

    }*/
}
