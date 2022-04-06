import java.sql.Connection;
import java.sql.SQLException;

import org.apache.arrow.vector.VectorSchemaRoot;

public interface DBDriverWrapper {

  Connection connect(String host, String database, String userName, String password);

  VectorSchemaRoot executeQuery(String sqlQuery) throws SQLException;
}
