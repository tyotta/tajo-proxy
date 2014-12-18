package org.apache.tajo.jdbc.proxy;

import org.apache.tajo.catalog.Schema;
import org.apache.tajo.common.TajoDataTypes.Type;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.jdbc.TajoResultSetBase;
import org.apache.tajo.storage.Tuple;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

public class TajoMetaDataResultSet extends TajoResultSetBase {
  private List<MetaDataTuple> values;

  public TajoMetaDataResultSet(List<String> columns, List<Type> types, List<MetaDataTuple> values) {
    init();
    schema = new Schema();

    int index = 0;
    if(columns != null) {
      for(String columnName: columns) {
        schema.addColumn(columnName, types.get(index++));
      }
    }
    this.values = values;
    totalRow = values == null ? 0 : values.size();
  }

  @Override
  protected Tuple nextTuple() throws IOException {
    if(curRow >= totalRow) {
      return null;
    }
    return values.get(curRow);
  }

  @Override
  public void close() throws SQLException {
  }

  @Override
  public String getString(int fieldId) throws SQLException {
    Datum datum = cur.get(fieldId - 1);
    if(datum == null) {
      return null;
    }

    return datum.asChars();
  }

  @Override
  public String getString(String name) throws SQLException {
    Datum datum = cur.get(findColumn(name));
    if(datum == null) {
      return null;
    }
    return datum.asChars();
  }
}
