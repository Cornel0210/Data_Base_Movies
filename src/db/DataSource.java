package db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

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
}
