import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.internal.matchers.Any;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.stubbing.Answer;

@ExtendWith(MockitoExtension.class)
@RunWith(JUnitPlatform.class)
class PostgresDriverWrapperTest {
  @Mock
  private Connection mockConnection;
  @InjectMocks
  private PostgresDriverWrapper postgresDriverWrapper;
  @Mock
  private PreparedStatement mockPreparedStatement;
  @Mock
  private ResultSet mockResultSet;
  @Mock
  private ResultSetMetaData mockResultSetMetadata;

  @BeforeEach
  void setUp() throws SQLException {
    Mockito.when(mockPreparedStatement.executeQuery()).thenReturn(mockResultSet);
    Mockito.when(mockResultSet.getMetaData()).thenReturn(mockResultSetMetadata);
    Mockito.when(mockResultSet.next()).thenAnswer(new Answer() {
      private int iterations = 100;

      @Override
      public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
        return iterations-- > 0;
      }
    });

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
    try (MockedStatic<DriverManager> driverManagerMockedStatic = Mockito.mockStatic(DriverManager.class)) {
      driverManagerMockedStatic.when(() -> DriverManager.getConnection(Mockito.any(), Mockito.any()))
          .thenReturn(mockConnection);
      Mockito.when(mockConnection.isClosed()).thenReturn(false);
      postgresDriverWrapper.connect();
      Assertions.assertTrue(postgresDriverWrapper.isConnected());
    }
  }

  @Test
  void testExecuteQueryAll() throws SQLException {
    try (MockedStatic<DriverManager> driverManagerMockedStatic = Mockito.mockStatic(DriverManager.class)) {
      // Mocks setup
      driverManagerMockedStatic.when(() -> DriverManager.getConnection(Mockito.any(), Mockito.any()))
          .thenReturn(mockConnection);
      String sqlQuery = "select * from personaldata";
      Mockito.when(mockConnection.prepareStatement(sqlQuery)).thenReturn(mockPreparedStatement);
      Mockito.when(mockResultSet.getInt(Mockito.any())).thenReturn(1);
      Mockito.when(mockResultSet.getDouble(Mockito.any())).thenReturn(1.0);
      Mockito.when(mockResultSet.getString(Mockito.any())).thenReturn("test");
      Mockito.when(mockResultSetMetadata.getColumnCount()).thenReturn(4);
      Mockito.when(mockResultSetMetadata.getColumnName(1)).thenReturn("id");
      Mockito.when(mockResultSetMetadata.getColumnName(2)).thenReturn("name");
      Mockito.when(mockResultSetMetadata.getColumnName(3)).thenReturn("numberrange");
      Mockito.when(mockResultSetMetadata.getColumnName(4)).thenReturn("currency");
      Mockito.when(mockResultSetMetadata.getColumnTypeName(1)).thenReturn("serial");
      Mockito.when(mockResultSetMetadata.getColumnTypeName(2)).thenReturn("varchar");
      Mockito.when(mockResultSetMetadata.getColumnTypeName(3)).thenReturn("float8");
      Mockito.when(mockResultSetMetadata.getColumnTypeName(4)).thenReturn("varchar");

      postgresDriverWrapper.connect();
      try (VectorSchemaRoot vectorSchemaRoot = postgresDriverWrapper.executeQuery(sqlQuery)) {
        Assertions.assertEquals(vectorSchemaRoot.getRowCount(), 100);
        Assertions.assertEquals(vectorSchemaRoot.getFieldVectors().size(), 4);
        for (FieldVector fieldVector : vectorSchemaRoot.getFieldVectors()) {
          for (int i = 0; i < 100; i++) {
            Assertions.assertNotNull(fieldVector.getObject(i));
          }
        }
      }
    }
  }

  @Test
  void testExecuteQueryColumns() throws SQLException {
    try (MockedStatic<DriverManager> driverManagerMockedStatic = Mockito.mockStatic(DriverManager.class)) {
      // Mocks setup
      driverManagerMockedStatic.when(() -> DriverManager.getConnection(Mockito.any(), Mockito.any()))
          .thenReturn(mockConnection);
      String sqlQuery = "select name, currency from personaldata";
      Mockito.when(mockConnection.prepareStatement(sqlQuery)).thenReturn(mockPreparedStatement);
      Mockito.when(mockResultSet.getString(Mockito.any())).thenReturn("test");
      Mockito.when(mockResultSetMetadata.getColumnCount()).thenReturn(2);
      Mockito.when(mockResultSetMetadata.getColumnName(1)).thenReturn("name");
      Mockito.when(mockResultSetMetadata.getColumnName(2)).thenReturn("currency");
      Mockito.when(mockResultSetMetadata.getColumnTypeName(1)).thenReturn("varchar");
      Mockito.when(mockResultSetMetadata.getColumnTypeName(2)).thenReturn("varchar");

      postgresDriverWrapper.connect();
      try (VectorSchemaRoot vectorSchemaRoot = postgresDriverWrapper.executeQuery(
          sqlQuery)) {
        Assertions.assertEquals(vectorSchemaRoot.getRowCount(), 100);
        Assertions.assertEquals(vectorSchemaRoot.getFieldVectors().size(), 2);
        for (FieldVector fieldVector : vectorSchemaRoot.getFieldVectors()) {
          for (int i = 0; i < 100; i++) {
            Assertions.assertNotNull(fieldVector.getObject(i));
          }
        }
      }
    }
  }
}
