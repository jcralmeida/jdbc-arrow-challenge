import java.sql.SQLException;

import org.apache.arrow.vector.VectorSchemaRoot;

public interface DBDriverWrapper {

  VectorSchemaRoot executeQuery(String sqlQuery) throws SQLException;
}
