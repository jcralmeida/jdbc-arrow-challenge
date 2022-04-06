import java.sql.SQLException;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;

public class Main {
  public static void main(String[] args) throws SQLException {
    String userName = args[0];
    String password = args[1];
    String urlPort = args[2];
    String database = args[3];

    DBDriverWrapper driver = new PostgresDriverWrapper(urlPort, database, userName, password);
    VectorSchemaRoot vectorSchemaRoot = driver.executeQuery("select * from personaldata");
    driver.getConnection().close();

    for (FieldVector fieldVector : vectorSchemaRoot.getFieldVectors()) {
      for (int i = 0; i < vectorSchemaRoot.getRowCount(); i++) {
        System.out.println(fieldVector.getObject(i));
      }
    }
    vectorSchemaRoot.close();
  }
}
