import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;

public class VectorSchemaRootWrapper {
  static private final RootAllocator rootAllocator = new RootAllocator(Long.MAX_VALUE);

  private final VectorSchemaRoot vectorSchemaRoot;

  public VectorSchemaRootWrapper(Schema schema) {
    vectorSchemaRoot = VectorSchemaRoot.create(schema, rootAllocator);
  }

  public void addDouble(double item, String fieldName, int count) {
    for (FieldVector fieldVector :
        vectorSchemaRoot.getFieldVectors()) {
      if (fieldName.equals(fieldVector.getName())) {
        if (fieldVector instanceof Float8Vector) {
          ((Float8Vector) fieldVector).setSafe(count, item);
        }
      }

    }
  }

  public void addVarchar(String item, String fieldName, int count) {
    for (FieldVector fieldVector :
        vectorSchemaRoot.getFieldVectors()) {
      if (fieldName.equals(fieldVector.getName())) {
        if (fieldVector instanceof VarCharVector) {
          ((VarCharVector) fieldVector).setSafe(count, new Text(item));
        }
      }

    }
  }

  public VectorSchemaRoot getVectorSchemaRoot() {
    return vectorSchemaRoot;
  }

}
