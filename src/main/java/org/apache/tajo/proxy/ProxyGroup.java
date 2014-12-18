package org.apache.tajo.proxy;

import org.apache.tajo.proxy.ipc.ProxyServerClientProtocol.ProxyGroupProto;

import java.util.*;

public class ProxyGroup {
  private String groupName;
  private Set<String> tables;

  public ProxyGroup() {
    tables = new TreeSet<String>();
  }

  public ProxyGroup(ProxyGroupProto proto) {
    groupName = proto.getGroupName();
    tables = new TreeSet<String>();

    for (String eachTableName: proto.getTableNamesList()) {
      tables.add(eachTableName);
    }
  }

  public String getGroupName() {
    return groupName;
  }

  public void setGroupName(String groupName) {
    this.groupName = groupName;
  }

  public Set<String> getTables() {
    return tables;
  }

  public void setTables(Set<String> tables) {
    this.tables = tables;
  }

  public ProxyGroupProto getProto() {
    ProxyGroupProto.Builder builder = ProxyGroupProto.newBuilder();
    builder.setGroupName(groupName);
    builder.addAllTableNames(new ArrayList<String>(tables));
    return builder.build();
  }

  public boolean equals(Object obj) {
    if (!(obj instanceof ProxyGroup)) {
      return false;
    }

    return groupName.equals(((ProxyGroup)obj).groupName);
  }

  public String toString() {
    return toFormatString();
  }

  public String toFormatString() {
    String result = groupName;
    String prefix = ": [";
    for (String eachTables: tables) {
      result += prefix + eachTables;
      prefix = ", ";
    }
    if (!tables.isEmpty()) {
      result += "]";
    } else {
      result += ": [no granted table]";
    }
    return result;
  }

  public boolean hasTable(String tableName) {
    if (tables.contains("*") || tables.contains(tableName)) {
      return true;
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return groupName.hashCode();
  }
}
