import static java.util.Arrays.asList;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.postgresql.Driver;

public class PostgresDriverWrapper implements DBDriverWrapper{
  static private final String URL_BASE = "jdbc:postgresql://";  
  static private final Driver DRIVER = new Driver();
  private final Connection connection;

  public PostgresDriverWrapper(String host, String database, String userName, String password) {
    connection = connect(host, database, userName, password);
  }

  @Override
  public Connection connect(String host, String database, String userName, String password) {
    Properties props = new Properties();
    props.setProperty("user", userName);
    props.setProperty("password", password);
    String fullURL = URL_BASE + host + "/postgres";
    try {
      return DRIVER.connect(fullURL, props);
    } catch (SQLException e) {
      throw new RuntimeException("Couldn't connect to DB server");
    }
  }

  @Override
  public VectorSchemaRoot executeQuery(String sqlQuery) throws SQLException {
    ResultSet result;
    try {
      PreparedStatement preparedStatement = connection.prepareStatement(sqlQuery);
      result = preparedStatement.executeQuery();
    } catch (SQLException e) {
      throw new RuntimeException();
    }
    Schema schema = new Schema(asList(
        Field.nullable("name", Types.MinorType.VARCHAR.getType()),
        Field.nullable("numberrange", Types.MinorType.FLOAT8.getType()),
        Field.nullable("currency", Types.MinorType.VARCHAR.getType())
    ));
    VectorSchemaRootWrapper vectorSchemaRootWrapper = new VectorSchemaRootWrapper(schema);
    int rowCount = 0;
    while (result.next()){
      vectorSchemaRootWrapper.addVarchar(result.getString("name"), "name", rowCount);
      vectorSchemaRootWrapper.addDouble(result.getDouble("numberrange"), "numberrange", rowCount);
      vectorSchemaRootWrapper.addVarchar(result.getString("currency"), "currency", rowCount);
      rowCount++;
    }
    vectorSchemaRootWrapper.getVectorSchemaRoot().setRowCount(rowCount);
    return vectorSchemaRootWrapper.getVectorSchemaRoot();
  }

  public Connection getConnection() {
    return connection;
  }
}
