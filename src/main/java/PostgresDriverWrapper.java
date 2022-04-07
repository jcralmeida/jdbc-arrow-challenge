import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

public class PostgresDriverWrapper implements DBDriverWrapper, AutoCloseable {
  private final String url_base = "jdbc:postgresql://";
  private final String username;
  private final String host;
  private final String password;
  private Connection connection;

  public PostgresDriverWrapper(String host, String username, String password) {
    this.username = username;
    this.host = host;
    this.password = password;
  }

  public Connection connect() throws SQLException {
    Properties props = new Properties();
    props.setProperty("user", username);
    props.setProperty("password", password);
    String fullURL = url_base + host + "/postgres";
    connection = DriverManager.getConnection(fullURL, props);
    return connection;
  }

  public ArrowType getArrowTypeFromName(String typeName) {
    switch (typeName) {
      case "varchar":
        return Types.MinorType.VARCHAR.getType();
      case "float8":
        return Types.MinorType.FLOAT8.getType();
      case "float4":
        return Types.MinorType.FLOAT4.getType();
      case "serial":
        return Types.MinorType.INT.getType();
      default:
        return Types.MinorType.NULL.getType();
    }
  }

  @Override
  public VectorSchemaRoot executeQuery(String sqlQuery) throws SQLException {
    try (PreparedStatement preparedStatement = connection.prepareStatement(sqlQuery);
         ResultSet result = preparedStatement.executeQuery()) {
      ResultSetMetaData resultSetMetaData = result.getMetaData();
      int vectorCount = resultSetMetaData.getColumnCount();

      List<Field> fields = new ArrayList<>();
      for (int i = 1; i <= vectorCount; i++) {
        fields.add(Field.nullable(resultSetMetaData.getColumnName(i), getArrowTypeFromName(
            resultSetMetaData.getColumnTypeName(i))));
      }
      Schema schema = new Schema(fields);
      VectorSchemaRootWrapper vectorSchemaRootWrapper = new VectorSchemaRootWrapper(schema);

      int rowCount = 0;
      while (result.next()) {
        for (Field field : schema.getFields()) {
          if (field.getType().equals(Types.MinorType.VARCHAR.getType())) {
            vectorSchemaRootWrapper.addVarchar(result.getString(field.getName()), field.getName(), rowCount);
          } else if (field.getType().equals(Types.MinorType.FLOAT8.getType())) {
            vectorSchemaRootWrapper.addDouble(result.getDouble(field.getName()), field.getName(), rowCount);
          } else if (field.getType().equals(Types.MinorType.INT.getType())) {
            vectorSchemaRootWrapper.addInt(result.getInt(field.getName()), field.getName(), rowCount);
          }
        }
        rowCount++;
        vectorSchemaRootWrapper.updateRowCount(rowCount);
      }
      return vectorSchemaRootWrapper.getVectorSchemaRoot();
    }
  }

  @Override
  public void close() throws Exception {
    connection.close();
  }

  public boolean isConnected() throws SQLException {
    return !connection.isClosed();
  }
}
