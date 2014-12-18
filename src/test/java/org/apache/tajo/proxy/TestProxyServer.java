
package org.apache.tajo.proxy;

import com.google.protobuf.ByteString;
import org.apache.tajo.QueryId;
import org.apache.tajo.TajoConstants;
import org.apache.tajo.TajoIdProtos;
import org.apache.tajo.TajoProtos;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.ipc.ClientProtos;
import org.apache.tajo.ipc.ClientProtos.GetTableListRequest;
import org.apache.tajo.ipc.ClientProtos.GetTableListResponse;
import org.apache.tajo.proxy.ProxyClientRpcService.ProxyServerClientProtocolHandler;
import org.apache.tajo.proxy.ProxyClientRpcService.ResultSetHolder;
import org.apache.tajo.proxy.ipc.ProxyServerClientProtocol;
import org.apache.tajo.proxy.ipc.ProxyServerClientProtocol.CreateProxySessionRequest;
import org.apache.tajo.proxy.ipc.ProxyServerClientProtocol.ProxyUserProto;
import org.apache.tajo.storage.RowStoreUtil;
import org.apache.tajo.storage.Tuple;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNotNull;
import static org.junit.Assert.*;

public class TestProxyServer {
  private static TajoProxyServerTestCluster cluster;
  private static ProxyServerClientProtocolHandler protocolHandler;
  private static TajoIdProtos.SessionIdProto sessionId;

  @BeforeClass
  public static void setUp() throws Exception {
    cluster = new TajoProxyServerTestCluster();
    cluster.startCluster();
    protocolHandler = cluster.getTajoProxyServer().getClientRpcService().getProtocolHandler();

    CreateProxySessionRequest request = CreateProxySessionRequest.newBuilder()
        .setDefaultDatabase(TajoConstants.DEFAULT_DATABASE_NAME)
        .setUser(
            ProxyUserProto.newBuilder().setUserId("admin")
                .setPassword(ProxyUserAdmin.encodingPassword("admin")).build()).build();

    sessionId = protocolHandler.createSession(null, request).getSessionId();
  }

  @AfterClass
  public static void tearDown() {
    if (cluster != null) {
      cluster.stopCluster();
    }
  }
  
  @Test
  public final void testGetTableList() throws Exception {
    GetTableListRequest.Builder builder = GetTableListRequest.newBuilder();
    builder.setSessionId(sessionId);
    builder.setDatabaseName(TajoConstants.DEFAULT_DATABASE_NAME);
    GetTableListResponse res = protocolHandler.getTableList(null, builder.build());
    assertNotNull(res);

    assertEquals(9, res.getTablesList().size());
  }

  @Test
  public final void testSubmitQueryAndResult() throws Exception {
    String query = "select * from lineitem";

    CreateProxySessionRequest request = CreateProxySessionRequest.newBuilder()
        .setDefaultDatabase(TajoConstants.DEFAULT_DATABASE_NAME)
        .setUser(
            ProxyUserProto.newBuilder().setUserId("admin")
                .setPassword(ProxyUserAdmin.encodingPassword("admin")).build()).build();

    ProxyServerClientProtocol.ServerResponse serverResponse =
        protocolHandler.createSession(null, request);
    assertNotNull(serverResponse);
    assertEquals(ClientProtos.ResultCode.OK, serverResponse.getResultCode());
    assertTrue(serverResponse.hasSessionId());

    TajoIdProtos.SessionIdProto sessionId = serverResponse.getSessionId();

    ClientProtos.GetQueryStatusResponse response = protocolHandler.submitQuery(null, ClientProtos.QueryRequest.newBuilder()
        .setQuery(query)
        .setSessionId(sessionId)
        .build());

    assertNotNull(response);
    assertEquals(ClientProtos.ResultCode.OK, response.getResultCode());
    assertTrue(response.getErrorMessage() == null || response.getErrorMessage().isEmpty());
    assertNotNull(response.getQueryId());

    ProxyServerClientProtocol.ProxyQueryResult queryResult = null;

    long startTime = System.currentTimeMillis();
    while (true) {
      queryResult = protocolHandler.getQueryResult(null,
        ProxyServerClientProtocol.ProxyQueryResultRequest.newBuilder()
            .setResultRequestMeta(
                ClientProtos.GetQueryResultRequest.newBuilder()
                    .setQueryId(response.getQueryId())
                    .setSessionId(sessionId)
                    .build()
            )
            .setFetchSize(3)
            .build()
          );

      assertNotNull(queryResult);
      assertNotNull(queryResult.getQueryStatus());

      if (queryResult.getQueryStatus().getState() == TajoProtos.QueryState.QUERY_SUCCEEDED) {
        break;
      }

      String errorMessage = queryResult.getQueryStatus().getErrorMessage();
      if (errorMessage != null && !errorMessage.isEmpty()) {
        fail("Received error message from server:" + errorMessage + ", state=" + queryResult.getQueryStatus().getState());
      }

      if (System.currentTimeMillis() - startTime > 20 * 1000) {
        fail("Too long time query execution.");
        break;
      }

      Thread.sleep(2 * 1000);
    }

    //assert first 3 row
    assertQueryResult(queryResult, 3);

    //fetch remain result and assert
    queryResult = protocolHandler.getQueryResult(null,
        ProxyServerClientProtocol.ProxyQueryResultRequest.newBuilder()
            .setResultRequestMeta(
                ClientProtos.GetQueryResultRequest.newBuilder()
                    .setQueryId(response.getQueryId())
                    .setSessionId(sessionId)
                    .build()
            )
            .setFetchSize(10)
            .build()
    );

    assertQueryResult(queryResult, 2);

    ProxyServerClientProtocol.ServerResponse closeResponse =
        protocolHandler.closeQuery(null,
            ProxyServerClientProtocol.CloseQueryRequest.newBuilder()
                .setQueryId(queryResult.getQueryStatus().getQueryId())
                .setSessionId(sessionId)
                .build()
            );

    assertEquals(ClientProtos.ResultCode.OK, closeResponse.getResultCode());
    assertFalse(closeResponse.hasErrorMessage());

    QueryId queryId = new QueryId(queryResult.getQueryStatus().getQueryId());
    assertNull(cluster.getTajoProxyServer().getClientRpcService().getResultSetHolder(
        ResultSetHolder.getKey(sessionId, queryId)));
    assertNull(cluster.getTajoProxyServer().getClientRpcService().getQuerySubmitTask(sessionId.toString() + queryId.toString()));
  }

  private void assertQueryResult(ProxyServerClientProtocol.ProxyQueryResult queryResult, int numRows) throws Exception {
    CatalogProtos.TableDescProto tableDesc = queryResult.getTableDesc();
    assertNotNull(tableDesc);

    List<ByteString> rows = queryResult.getRowsList();
    assertNotNull(rows);
    assertEquals(numRows, rows.size());

    Schema schema = new Schema(tableDesc.getSchema());
    RowStoreUtil.RowStoreDecoder rowDecoder = RowStoreUtil.createDecoder(schema);
    for (ByteString eachRow: rows) {
      Tuple rowTuple = rowDecoder.toTuple(eachRow.toByteArray());
      assertEquals(schema.getColumns().size(), rowTuple.size());
      for (int i = 0; i < rowTuple.size(); i++) {
        Datum datum = rowTuple.get(i);
        assertNotNull(datum);
      }
    }
  }
}
