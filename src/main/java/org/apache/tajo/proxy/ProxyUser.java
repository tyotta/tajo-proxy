package org.apache.tajo.proxy;

import org.apache.tajo.TajoIdProtos.SessionIdProto;
import org.apache.tajo.proxy.ipc.ProxyServerClientProtocol.ProxyGroupProto;
import org.apache.tajo.proxy.ipc.ProxyServerClientProtocol.ProxyUserProto;

import java.util.ArrayList;
import java.util.List;

public class ProxyUser {
  private String userId;
  private String password;
  private List<ProxyGroup> groups;

  public ProxyUser() {
    groups = new ArrayList<ProxyGroup>();
  }

  public ProxyUser(ProxyUserProto userProto) {
    userId = userProto.getUserId();
    password = userProto.getPassword();

    groups = new ArrayList<ProxyGroup>();

    for (ProxyGroupProto eachGroupProto: userProto.getGroupsList()) {
      ProxyGroup group = new ProxyGroup(eachGroupProto);
      groups.add(group);
    }
  }

  public String getUserId() {
    return userId;
  }

  public void setUserId(String userId) {
    this.userId = userId;
  }

  public String getPassword() {
    return password;
  }

  public void setPassword(String password) {
    this.password = password;
  }

  public List<ProxyGroup> getGroups() {
    return groups;
  }

  public void setGroups(List<ProxyGroup> groups) {
    this.groups = groups;
  }

  public ProxyUserProto getProto(SessionIdProto sessionId) {
    ProxyUserProto.Builder builder = ProxyUserProto.newBuilder();

    builder.setSessionId(sessionId);
    builder.setUserId(userId);
    try {
      builder.setPassword(ProxyUserAdmin.encodingPassword(password));
    } catch (Exception e) {
      throw new RuntimeException(e.getMessage(), e);
    }

    List<ProxyGroupProto> groupList = new ArrayList<ProxyGroupProto>();
    for (ProxyGroup eachGroup: groups) {
      groupList.add(eachGroup.getProto());
    }
    builder.addAllGroups(groupList);
    return builder.build();
  }

  public boolean equals(Object obj) {
    if (!(obj instanceof ProxyUser)) {
      return false;
    }

    return userId.equals(((ProxyUser)obj).userId);
  }

  public String toString() {
    return toFormatString();
  }

  public String toFormatString() {
    String result = userId;
    String prefix = ": [";
    for (ProxyGroup eachGroup: groups) {
      result += prefix + eachGroup.getGroupName();
      prefix = ", ";
    }
    if (!groups.isEmpty()) {
      result += "]";
    } else {
      result += ": [no group]";
    }
    return result;
  }

  public String toGroupString() {
    String result = "";
    String prefix = "[";
    for (ProxyGroup eachGroup: groups) {
      result += prefix + eachGroup.getGroupName();
      prefix = ", ";
    }
    if (!groups.isEmpty()) {
      result += "]";
    } else {
      result += ": [no group]";
    }
    return result;
  }
}
