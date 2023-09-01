import db.DataSource;

public class Main {
    public static void main(String[] args) {
        DataSource dataSource = new DataSource();
        boolean isOpen = dataSource.openConn();
        if (!isOpen) {
            System.out.println("Cannot open the connection");
        }


        if (isOpen) {
            dataSource.closeConn();
        }
    }
}
