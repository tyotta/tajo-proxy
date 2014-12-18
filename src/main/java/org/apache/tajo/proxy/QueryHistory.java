package org.apache.tajo.proxy;

import org.apache.tajo.QueryId;
import org.apache.tajo.ipc.ClientProtos.BriefQueryInfo;

public class QueryHistory {
  private String queryId;
  private String state;
  private long startTime;
  private long finishTime;
  private String query;
  private String queryMasterHost;
  private int queryMasterPort;
  private float progress;
  private String userId;

  private QueryExternalParam externalParam;

  public QueryHistory() {
  }

  public QueryHistory(BriefQueryInfo queryInfo) {
    this.queryId = new QueryId(queryInfo.getQueryId()).toString();
    this.state = queryInfo.getState().name();
    this.startTime = queryInfo.getStartTime();
    this.finishTime = queryInfo.getFinishTime();
    this.query = queryInfo.getQuery();
    this.queryMasterHost = queryInfo.getQueryMasterHost();
    this.queryMasterPort = queryInfo.getQueryMasterPort();
    this.progress = queryInfo.getProgress();
  }

  public String getQueryId() {
    return queryId;
  }

  public void setQueryId(String queryId) {
    this.queryId = queryId;
  }

  public String getState() {
    return state;
  }

  public void setState(String state) {
    this.state = state;
  }

  public long getStartTime() {
    return startTime;
  }

  public void setStartTime(long startTime) {
    this.startTime = startTime;
  }

  public long getFinishTime() {
    return finishTime;
  }

  public void setFinishTime(long finishTime) {
    this.finishTime = finishTime;
  }

  public String getQuery() {
    return query;
  }

  public void setQuery(String query) {
    this.query = query;
  }

  public String getQueryMasterHost() {
    return queryMasterHost;
  }

  public void setQueryMasterHost(String queryMasterHost) {
    this.queryMasterHost = queryMasterHost;
  }

  public int getQueryMasterPort() {
    return queryMasterPort;
  }

  public void setQueryMasterPort(int queryMasterPort) {
    this.queryMasterPort = queryMasterPort;
  }

  public float getProgress() {
    return progress;
  }

  public void setProgress(float progress) {
    this.progress = progress;
  }

  public String getUserId() {
    return userId;
  }

  public void setUserId(String userId) {
    this.userId = userId;
  }

  public QueryExternalParam getQueryExternalParam() {
    return externalParam;
  }

  public void setExternalParam(QueryExternalParam externalParam) {
    this.externalParam = externalParam;
  }

  @Override
  public String toString() {
    return queryId + "," + userId + "," + state + "," + progress + ", [" + externalParam + "]";
  }
}
