package org.apache.tajo.proxy;

public class QueryExternalParam {
  String submitTime;
  String userId;
  String queryType;
  String arg1;
  String arg2;
  String realQuery;

  public String getQueryId() {
    return submitTime + "_" + userId + "_" + queryType + "_" + arg1 + "_" + arg2;
  }

  public QueryExternalParam clone() {
    QueryExternalParam cloneParam = new QueryExternalParam();
    cloneParam.submitTime = submitTime;
    cloneParam.userId = userId;
    cloneParam.queryType = queryType;
    cloneParam.arg1 = arg1;
    cloneParam.arg2 = arg2;

    return cloneParam;
  }

  public String getUserId() {
    return userId;
  }

  public String getRealQuery() {
    return realQuery;
  }

  public String getSubmitTime() {
    return submitTime;
  }

  public String getQueryType() {
    return queryType;
  }

  public String getArg1() {
    return arg1;
  }

  public String getArg2() {
    return arg2;
  }

  public void setSubmitTime(String submitTime) {
    this.submitTime = submitTime;
  }

  public void setUserId(String userId) {
    this.userId = userId;
  }

  public void setQueryType(String queryType) {
    this.queryType = queryType;
  }

  public void setArg1(String arg1) {
    this.arg1 = arg1;
  }

  public void setArg2(String arg2) {
    this.arg2 = arg2;
  }

  public void setRealQuery(String realQuery) {
    this.realQuery = realQuery;
  }

  @Override
  public String toString() {
    return getQueryId();
  }
}
