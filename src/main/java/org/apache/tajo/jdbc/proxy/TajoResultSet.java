package org.apache.tajo.jdbc.proxy;

import com.google.protobuf.ByteString;
import org.apache.tajo.QueryId;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.NullDatum;
import org.apache.tajo.datum.TextDatum;
import org.apache.tajo.jdbc.TajoResultSetBase;
import org.apache.tajo.proxy.ipc.ProxyServerClientProtocol;
import org.apache.tajo.proxy.ipc.ProxyServerClientProtocol.ProxyQueryResult;
import org.apache.tajo.storage.RowStoreUtil;
import org.apache.tajo.storage.Tuple;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;

public class TajoResultSet extends TajoResultSetBase {
  private TajoClientInterface tajoProxyClient;
  private QueryId queryId;
  private ProxyServerClientProtocol.ProxyQueryResult queryResult;
  private List<ByteString> rowDatas;
  private Iterator<ByteString> rowIterator;
  private int fetchSize;
  private int maxRows;
  private long totalFetchRows;
  private RowStoreUtil.RowStoreDecoder rowDecoder;
  private String characterEncoding;

  public TajoResultSet(TajoClientInterface tajoProxyClient,
                       QueryId queryId,
                       String characterEncoding,
                       ProxyServerClientProtocol.ProxyQueryResult queryResult) {
    super(null);

    this.tajoProxyClient = tajoProxyClient;
    this.queryId = queryId;
    this.characterEncoding = characterEncoding;
    this.queryResult = queryResult;

    CatalogProtos.TableDescProto desc = queryResult.getTableDesc();
    this.schema = new Schema(queryResult.getTableDesc().getSchema());
    this.totalRow = desc.getStats() != null ? desc.getStats().getNumRows() : Integer.MAX_VALUE;
    if (this.totalRow <= 0) {
      //Case of select * from table
      this.totalRow = Integer.MAX_VALUE;
    }
    this.rowDatas = queryResult.getRowsList();
    this.rowIterator = rowDatas.iterator();
    this.rowDecoder = RowStoreUtil.createDecoder(schema);
  }

  @Override
  protected Tuple nextTuple() throws IOException {
    if (maxRows > 0 && totalFetchRows >= maxRows) {
      return null;
    }

    if (rowIterator.hasNext()) {
      ByteString row = rowIterator.next();
      totalFetchRows++;
      return rowDecoder.toTuple(row.toByteArray());
    } else {
      queryResult = tajoProxyClient.getProxyQueryResult(queryId, fetchSize);
      if (queryResult == null || queryResult.getRowsList().isEmpty()) {
        return null;
      } else {
        rowDatas = queryResult.getRowsList();
        rowIterator = rowDatas.iterator();
        return nextTuple();
      }
    }
  }

  @Override
  public void close() throws SQLException {
    if (rowDatas != null) {
      rowDatas = null;
      rowIterator = null;

      tajoProxyClient.closeQuery(queryId);
    }
  }

  @Override
  public void setFetchSize(int size) throws SQLException {
    this.fetchSize = size;
  }

  public void setMaxRows(int maxRows) {
    this.maxRows = maxRows;
  }

  public QueryId getQueryId() {
    return queryId;
  }

  public long getTotalRow() {
    return totalRow;
  }

  @Override
  public String getString(int fieldId) throws SQLException {
    try {
      Datum datum = cur.get(fieldId - 1);
      handleNull(datum);
      if (characterEncoding != null) {
        if (datum instanceof TextDatum) {
          return new String(datum.asByteArray(), characterEncoding);
        } else {
          return new String(datum.asChars().getBytes(), characterEncoding);
        }
      } else {
        return datum.asChars();
      }
    } catch (UnsupportedEncodingException e) {
      throw new SQLException(e.getMessage(), e);
    }
  }

  @Override
  public String getString(String name) throws SQLException {
    try {
      Datum datum = cur.get(findColumn(name));
      handleNull(datum);
      if (characterEncoding != null) {
        if (datum instanceof TextDatum) {
          return new String(datum.asByteArray(), characterEncoding);
        } else {
          return new String(datum.asChars().getBytes(), characterEncoding);
        }
      } else {
        return datum.asChars();
      }
    } catch (UnsupportedEncodingException e) {
      throw new SQLException(e.getMessage(), e);
    }
  }

  private void handleNull(Datum d) {
    wasNull = (d instanceof NullDatum);
  }

  public ProxyQueryResult getProxyQueryResult() {
    return queryResult;
  }
}
