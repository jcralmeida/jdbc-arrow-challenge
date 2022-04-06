import java.sql.Connection;
import java.sql.SQLException;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class PostgresDriverWrapperTest {
  private PostgresDriverWrapper postgresDriverWrapper;

  @BeforeEach
  void setUp() throws SQLException {
    String userName = "postgres";
    String password = "postgres";
    String urlPort = "localhost:5432";
    this.postgresDriverWrapper = new PostgresDriverWrapper(urlPort, userName, password);
  }

  @AfterEach
  void tearDown() throws Exception {
    postgresDriverWrapper.close();
  }

  @Test
  void testConnect() throws SQLException {
    String userName = "postgres";
    String password = "postgres";
    String urlPort = "localhost:5432";
    Connection connection = postgresDriverWrapper.connect(userName, password, urlPort);
    Assertions.assertNotNull(connection);
  }

  @Test
  void testExecuteQuery() throws SQLException {
    try (VectorSchemaRoot vectorSchemaRoot = postgresDriverWrapper.executeQuery("select * from personaldata")) {
      Assertions.assertEquals(vectorSchemaRoot.getRowCount(), 100);
      Assertions.assertEquals(vectorSchemaRoot.getFieldVectors().size(), 3);
      for (FieldVector fieldVector : vectorSchemaRoot.getFieldVectors()) {
        for (int i = 0; i < 100; i++) {
          Assertions.assertNotNull(fieldVector.getObject(i));
        }
      }
    }
  }
}
