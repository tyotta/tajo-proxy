package org.apache.tajo.proxy;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.proxy.ipc.ProxyServerClientProtocol.ProxyUserProto;
import java.io.InputStream;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class ProxyUserManageService {
  private final static Log LOG = LogFactory.getLog(ProxyUserManageService.class);

  private Configuration userConf;
  private Map<String, ProxyUser> sessionUsers = new HashMap<String, ProxyUser>();
  private Map<String, ProxyUser> allUsers = new HashMap<String, ProxyUser>();
  private Map<String, ProxyGroup> allGroups = new HashMap<String, ProxyGroup>();
  private Map<String, Set<String>> groupUsers = new HashMap<String, Set<String>>();
  private Map<String, Set<String>> groupTables = new HashMap<String, Set<String>>();

  private ProxyUserStore proxyUserStore;
  private UserMetaReloadThread userMetaReloadThread;

  public ProxyUserManageService() {
  }

  public void init(TajoConf tajoConf) throws Exception {
    Class clazz = tajoConf.getClass("tajo.proxy.user.store", LocalFileProxyUserStore.class);

    proxyUserStore = (ProxyUserStore)clazz.newInstance();
    proxyUserStore.init(tajoConf);

    loadTajoUsers();

    userMetaReloadThread = new UserMetaReloadThread();
    userMetaReloadThread.start();
  }

  public void loadTajoUsers() throws Exception {
    //TODO in the case of deleting user of revoking permission, should be checked session user.
    synchronized (allUsers) {
      allUsers.clear();
      allGroups.clear();
      groupUsers.clear();
      groupTables.clear();

      InputStream userDataInputStream = null;

      try {
        userDataInputStream = proxyUserStore.openForRead();

        userConf = new Configuration(false);
        userConf.addResource(userDataInputStream);

        String[] users = userConf.get("tajo.proxy.users", "admin").split(",");

        for (String eachUser : users) {
          String password = userConf.get("tajo.proxy.user." + eachUser + ".password",
              ProxyUserAdmin.encodingPassword(eachUser));   //default password is userId

          ProxyUser proxyUser = new ProxyUser();
          proxyUser.setUserId(eachUser);
          proxyUser.setPassword(password);

          allUsers.put(eachUser, proxyUser);
        }

        String[] groups = userConf.get("tajo.proxy.groups", "admin").split(",");
        for (String eachGroup : groups) {
          ProxyGroup group = new ProxyGroup();
          group.setGroupName(eachGroup);
          allGroups.put(eachGroup, group);

          String[] groupTableList = userConf.get("tajo.proxy.group." + eachGroup + ".tables", "").split(",");
          Set<String> groupTableSet = new TreeSet<String>();
          for (String eachTable : groupTableList) {
            if (eachTable.isEmpty()) {
              continue;
            }
            groupTableSet.add(eachTable);
          }
          group.setTables(groupTableSet);
          groupTables.put(eachGroup, groupTableSet);

          String[] groupUserList = userConf.get("tajo.proxy.group." + eachGroup + ".users", "").split(",");
          Set<String> groupUserSet = new HashSet<String>();
          for (String eachGroupUser : groupUserList) {
            if (!eachGroupUser.trim().isEmpty()) {
              groupUserSet.add(eachGroupUser);
            }
          }
          groupUsers.put(eachGroup, groupUserSet);

          for (String eachGroupUser : groupUserList) {
            if (allUsers.containsKey(eachGroupUser)) {
              allUsers.get(eachGroupUser).getGroups().add(group);
            }
          }
        }

        for (ProxyUser eachUser : allUsers.values()) {
          LOG.info("User loaded: " + eachUser.toFormatString());
        }

        for (ProxyGroup eachGroup : allGroups.values()) {
          LOG.info("Group loaded: " + eachGroup.toFormatString());
        }
      } finally {
        if (userDataInputStream != null) {
          userDataInputStream.close();
        }
      }
    }
  }

  public ProxyUser getProxyUser(String userId) {
    synchronized (allUsers) {
      return allUsers.get(userId);
    }
  }

  public Set<String> getGroupUsers(String groupName) {
    synchronized (allUsers) {
      return groupUsers.get(groupName);
    }
  }

  public Set<String> getUserGrantedTables(String sessionId) {
    synchronized (allUsers) {
      ProxyUser proxyUser = sessionUsers.get(sessionId);
      if (proxyUser == null) {
        LOG.warn("Not login user.");
        return new HashSet<String>();
      }

      Set<String> tables = new HashSet<String>();
      for (ProxyGroup eachGroup : proxyUser.getGroups()) {
        for (String eachTable : eachGroup.getTables()) {
          if ("*".equals(eachTable)) {
            tables.clear();
            tables.add("*");
            return tables;
          } else {
            tables.add(eachTable);
          }
        }
      }

      return tables;
    }
  }

  public boolean login(ProxyUserProto proxyUserProto) {
    synchronized (allUsers) {
      ProxyUser proxyUser = allUsers.get(proxyUserProto.getUserId());
      if (proxyUser == null) {
        LOG.info("Login failed: " + proxyUserProto.getUserId() + " not exists.");
        return false;
      }

      if (!proxyUser.getPassword().equals(proxyUserProto.getPassword())) {
        LOG.info("Login failed: " + proxyUserProto.getUserId() + " wrong password: " + proxyUserProto.getPassword());
        return false;
      }
      return true;
    }
  }

  public void addLoginUser(String sessionId, ProxyUserProto proxyUserProto) {
    synchronized (allUsers) {
      ProxyUser user = allUsers.get(proxyUserProto.getUserId());
      if (user == null) {
        LOG.warn("No user detail data in loaded user list: " + proxyUserProto.getUserId());
      }
      sessionUsers.put(sessionId, user);
    }
  }

  public ProxyUser getSessionUser(String sessionId) {
    synchronized (allUsers) {
      return sessionUsers.get(sessionId);
    }
  }

  public void logout(String sessionId) {
    synchronized (allUsers) {
      sessionUsers.remove(sessionId);
    }
  }

  public boolean isAdmin(String sessionId) {
    synchronized (allUsers) {
      ProxyUser proxyUser = sessionUsers.get(sessionId);
      if (proxyUser == null) {
        return false;
      }

      for (ProxyGroup group : proxyUser.getGroups()) {
        if (group.getGroupName().equals("admin")) {
          return true;
        }
      }

      return false;
    }
  }

  public boolean addGroup(String groupName) {
    synchronized (allUsers) {
      if (allGroups.containsKey(groupName)) {
        return false;
      }

      ProxyGroup group = new ProxyGroup();
      group.setGroupName(groupName);

      allGroups.put(groupName, group);
      groupTables.put(groupName, group.getTables());
      groupUsers.put(groupName, new HashSet<String>());

      writeUsersToXml();
      return true;
    }
  }

  public boolean deleteGroup(String groupName) {
    synchronized (allUsers) {
      if (!allGroups.containsKey(groupName)) {
        return false;
      }
      ProxyGroup group = allGroups.remove(groupName);

      groupTables.remove(groupName);
      Set<String> users = groupUsers.remove(groupName);
      if (users != null) {
        for (String eachUser: users) {
          ProxyUser user = allUsers.get(eachUser);
          if (user != null) {
            user.getGroups().remove(group);
          }
        }
      }
      writeUsersToXml();

      return true;
    }
  }

  public boolean deleteUser(ProxyUserProto proxyUserProto) {
    synchronized (allUsers) {
      if (!allUsers.containsKey(proxyUserProto.getUserId())) {
        return false;
      }

      ProxyUser user = allUsers.remove(proxyUserProto.getUserId());
      for (ProxyGroup eachGroup: user.getGroups()) {
        if (groupUsers.containsKey(eachGroup.getGroupName())) {
          groupUsers.get(eachGroup.getGroupName()).remove(user.getUserId());
        }
      }

      // TODO remove sessionUser
      // Notice sessionUsers's key is SessionId but proxyUserProto's sessionId is admin's sessionId

      writeUsersToXml();
    }

    return true;
  }

  public boolean addUser(ProxyUserProto proxyUserProto) {
    synchronized (allUsers) {
      if (allUsers.containsKey(proxyUserProto.getUserId())) {
        return false;
      }
      ProxyUser proxyUser = new ProxyUser(proxyUserProto);
      allUsers.put(proxyUser.getUserId(), proxyUser);

      for (ProxyGroup eachGroup: proxyUser.getGroups()) {
        Set<String> groupMembers = groupUsers.get(eachGroup.getGroupName());
        if (groupMembers == null) {
          groupMembers = new HashSet<String>();
          groupUsers.put(eachGroup.getGroupName(), groupMembers);
        }
        groupMembers.add(proxyUser.getUserId());
      }
      writeUsersToXml();
      return true;
    }
  }

  public void addTables(String groupName, List<String> tableNames) {
    synchronized (allUsers) {
      ProxyGroup group = allGroups.get(groupName);
      if (group == null) {
        return;
      }

      group.getTables().addAll(tableNames);
      groupTables.get(groupName).addAll(tableNames);
      writeUsersToXml();
    }
  }

  public void removeTables(String groupName, List<String> tableNames) {
    synchronized (allUsers) {
      ProxyGroup group = allGroups.get(groupName);
      if (group == null) {
        return;
      }

      group.getTables().removeAll(tableNames);
      groupTables.get(groupName).removeAll(tableNames);
      writeUsersToXml();
    }
  }

  public Collection<ProxyGroup> getProxyGroups() {
    synchronized (allUsers) {
      return allGroups.values();
    }
  }

  public Collection<ProxyUser> getProxyUsers() {
    synchronized (allUsers) {
      return allUsers.values();
    }
  }

  public boolean hasPermission(ProxyUser user, String table) {
    synchronized (allUsers) {
      for (ProxyGroup eachGroup: user.getGroups()) {
        if (eachGroup.hasTable(table)) {
          return true;
        }
      }

      return false;
    }
  }

  public void changePassword(String userId, String password) {
    synchronized (allUsers) {
      ProxyUser user = allUsers.get(userId);
      if (user == null) {
        return;
      }

      user.setPassword(password);

      writeUsersToXml();
    }
  }

  private void writeUsersToXml() {
    LOG.info("Saving Proxy User Data");
    userConf.clear();

    String prefix = "";
    String userList = "";

    for (ProxyUser eachUser: allUsers.values()) {
      userList += prefix + eachUser.getUserId();
      userConf.set("tajo.proxy.user." + eachUser.getUserId() + ".password", eachUser.getPassword());
      prefix = ",";
    }

    userConf.set("tajo.proxy.users", userList);

    prefix = "";
    String groupList = "";
    for (String groupName: groupUsers.keySet()) {
      groupList += prefix + groupName;

      String groupMembers = "";
      prefix = "";
      for (String eachUser: groupUsers.get(groupName)) {
        groupMembers += prefix + eachUser;
        prefix += ",";
      }
      userConf.set("tajo.proxy.group." + groupName + ".users", groupMembers);

      String groupTableList = "";
      prefix = "";
      if (groupTables.containsKey(groupName)) {
        for (String eachTable: groupTables.get(groupName)) {
          if (eachTable.isEmpty()) {
            continue;
          }
          groupTableList += prefix + eachTable;
          prefix += ",";
        }
        userConf.set("tajo.proxy.group." + groupName + ".tables", groupTableList);
      }

      prefix = ",";
    }

    userConf.set("tajo.proxy.groups", groupList);

    try {
      proxyUserStore.saveUsers(userConf);
    } catch (Exception e) {
      throw new RuntimeException(e.getMessage(), e);
    }
  }

  private AtomicBoolean closed = new AtomicBoolean(false);

  public void close() {
    if (closed.get()) {
      return;
    }
    userMetaReloadThread.interrupted();
  }

  public void addUsersToGroup(String groupName, List<String> userIds) {
    synchronized (allUsers) {
      ProxyGroup group = allGroups.get(groupName);
      if (group == null) {
        return;
      }
      Set<String> selectedGroupUsers = groupUsers.get(groupName);
      if (selectedGroupUsers != null) {
        selectedGroupUsers.addAll(userIds);
      }

      for (String eachUserId: userIds) {
        ProxyUser user = allUsers.get(eachUserId);
        if (user != null) {
          user.getGroups().add(group);
        }
      }
      writeUsersToXml();
    }
  }

  public void removeUsersFromGroup(String groupName, List<String> userIds) {
    synchronized (allUsers) {
      ProxyGroup group = allGroups.get(groupName);
      if (group == null) {
        return;
      }
      Set<String> selectedGroupUsers = groupUsers.get(groupName);
      if (selectedGroupUsers != null) {
        selectedGroupUsers.removeAll(userIds);
      }

      for (String eachUserId: userIds) {
        ProxyUser user = allUsers.get(eachUserId);
        if (user != null) {
          user.getGroups().remove(group);
        }
      }

      writeUsersToXml();
    }
  }

  public class UserMetaReloadThread extends Thread {
    public void run() {
      while(!closed.get()) {
        try {
          synchronized(this) {
            wait(60 * 1000);
          }
        } catch (InterruptedException e) {
        }
        if (closed.get()) {
          break;
        }
        try {
          if (proxyUserStore.isDataFileChanged()) {
            LOG.info("Reloading Proxy User Data.");
            loadTajoUsers();
          }
        } catch (Exception e) {
          LOG.error(e.getMessage(), e);
        }
      }
    }
  }
}
