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

public class PostgresDriverWrapper implements DBDriverWrapper, AutoCloseable{
  private final String url_base = "jdbc:postgresql://";
  private final Connection connection;

  public PostgresDriverWrapper(String host, String userName, String password) throws SQLException {
    connection = connect(host, userName, password);
  }

  public Connection connect(String host, String userName, String password) throws SQLException {
    Properties props = new Properties();
    props.setProperty("user", userName);
    props.setProperty("password", password);
    String fullURL = url_base + host + "/postgres";
    return new Driver().connect(fullURL, props);
  }

  @Override
  public VectorSchemaRoot executeQuery(String sqlQuery) throws SQLException {
    try (PreparedStatement preparedStatement = connection.prepareStatement(sqlQuery);
         ResultSet result = preparedStatement.executeQuery()) {
      Schema schema = new Schema(asList(
          Field.nullable("name", Types.MinorType.VARCHAR.getType()),
          Field.nullable("numberrange", Types.MinorType.FLOAT8.getType()),
          Field.nullable("currency", Types.MinorType.VARCHAR.getType())
      ));
      VectorSchemaRootWrapper vectorSchemaRootWrapper = new VectorSchemaRootWrapper(schema);
      int rowCount = 0;
      while (result.next()) {
        vectorSchemaRootWrapper.addVarchar(result.getString("name"), "name", rowCount);
        vectorSchemaRootWrapper.addDouble(result.getDouble("numberrange"), "numberrange", rowCount);
        vectorSchemaRootWrapper.addVarchar(result.getString("currency"), "currency", rowCount);
        rowCount++;
      }
      vectorSchemaRootWrapper.getVectorSchemaRoot().setRowCount(rowCount);
      return vectorSchemaRootWrapper.getVectorSchemaRoot();
    } catch (SQLException e) {
      throw new RuntimeException();
    }
  }

  @Override
  public void close() throws Exception {
    connection.close();
  }
}
