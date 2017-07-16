package ai.descent.sparks;

import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import javax.sql.DataSource;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

public abstract class SparkDescentBase {

    private static final String URL = "url";
    private static final String DRIVER = "driver";
    private static final String USER = "user";
    private static final String PASSWORD = "password";
    private static final String PROP_FILE = "database.properties";


    private SparkSession spark;
    private DataSource dataSource;
    private Properties dbProperties;

    public SparkDescentBase(String appName) {
        this.spark = SparkSession.builder().
                config("spark.master", "local").
                config("spark.sql.codegen.wholeStage", "false").
//                master("localhost[*]").
        appName(appName).getOrCreate();

        this.dbProperties = getJdbcProperties();
        this.dataSource = createConnectionPool();
    }

    private DataSource createConnectionPool() {
        Properties p = this.dbProperties;
        System.out.println(p);
        BasicDataSource ds = new BasicDataSource();
        ds.setDriverClassName(p.getProperty(DRIVER));
        ds.setUrl(p.getProperty(URL));
        ds.setUsername(p.getProperty(USER));
        ds.setPassword(p.getProperty(PASSWORD));
        return ds;
    }

    protected Connection getConnection() {
        try {
            return dataSource.getConnection();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public abstract void run();

    protected SparkSession getSpark() {
        return spark;
    }

    protected Properties getJdbcProperties() {
        try {
            Properties p = new Properties();
            p.load(new FileReader(PROP_FILE));
            p.setProperty(PASSWORD, getPassword());
            return p;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    protected String getJdbcURL() {
        return this.dbProperties.getProperty(URL);
    }

    public Dataset<Row> getDataFrameReader(String table) {
        Properties props = getJdbcProperties();
        return this.spark.read()
                .jdbc(props.getProperty(URL), table, props);
    }

    private String getPassword() {
        String password = System.getProperty("password");
        if (password == null) {
            throw new IllegalArgumentException("You must specify -Dpassword=... to connect to the DB");
        }
        return password;
    }
}
