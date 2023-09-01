import db.DataSource;

public class Main {
    public static void main(String[] args) {
        DataSource dataSource = new DataSource();
        boolean isOpen = dataSource.openConn();
        if (!isOpen) {
            System.out.println("Cannot open the connection");
        }
        dataSource.createTables();
        dataSource.execute("CREATE TABLE IF NOT EXISTS awards (award BOOLEAN NOT NULL CHECK (award IN (0,1)))");
        //dataSource.dropTable("awards");

        if (isOpen) {
            dataSource.closeConn();
        }
    }
}
