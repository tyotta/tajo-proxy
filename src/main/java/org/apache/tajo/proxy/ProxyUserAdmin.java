package org.apache.tajo.proxy;

import jline.console.ConsoleReader;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.jdbc.proxy.TajoProxyClient;
import org.apache.tajo.util.TUtil;
import sun.misc.BASE64Encoder;

import java.io.IOException;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.List;

public class ProxyUserAdmin {
  private TajoProxyClient tajoProxyClient;
  private TajoConf conf;
  ConsoleReader reader;
  String hLine = "================================================";

  public ProxyUserAdmin() throws Exception {
    TajoConf conf = new TajoConf();

    List<String> proxyServers = null;
    if (conf.get(TajoProxyClient.TAJO_PROXY_SERVERS) != null) {
      proxyServers = TUtil.newList(conf.get(TajoProxyClient.TAJO_PROXY_SERVERS).split(","));
    }

    if (proxyServers == null || proxyServers.isEmpty()) {
      System.out.println("No tajo.proxy.servers in conf/tajo-site.xml");
      System.exit(0);
    }

    tajoProxyClient = new TajoProxyClient(conf, proxyServers, null);

    conf = new TajoConf(tajoProxyClient.getConf());
    conf.addResource("tajo-user.xml");

    reader = new ConsoleReader(System.in, System.out);
  }

  private int selectMenu() throws Exception {
    System.out.println("[1] List users");
    System.out.println("[2] Add user");
    System.out.println("[3] Delete user");
    System.out.println("[4] Change password");
    System.out.println("[5] List groups");
    System.out.println("[6] Add group");
    System.out.println("[7] Delete group");
    System.out.println("[8] Add users to a group");
    System.out.println("[9] Remove user from a group");
    System.out.println("[10] Grant tables to a group");
    System.out.println("[11] Revoke tables from a group");
    System.out.println("[q] Exit");
    String line = reader.readLine("Choose one of the commands: ");
    if (line == null || line.trim().isEmpty()) {
      return -1;
    }

    if ("q".equalsIgnoreCase(line.trim())) {
      return 0;
    }
    try {
      return Integer.parseInt(line);
    } catch (NumberFormatException e) {
      return -1;
    }
  }

  public void run()  {
    try {
      if (!loginAdmin()) {
        System.out.println("Admin failed to login.");
        System.exit(0);
      }

      while (true) {
        try {
          int menu = selectMenu();
          switch (menu) {
            case 1:
              listUsers();
              break;
            case 2:
              addUser();
              break;
            case 3:
              delUser();
              break;
            case 4:
              passwordUser();
              break;
            case 5:
              listGroups();
              break;
            case 6:
              addGroup();
              break;
            case 7:
              deleteGroup();
              break;
            case 8:
              addUserToGroup();
              break;
            case 9:
              removeUserFromGroup();
              break;
            case 10:
              grantTables();
              break;
            case 11:
              revokeTables();
              break;
            case 0:
              System.exit(0);
            default:
              break;
          }
        } catch (Exception e) {
          System.out.println("ERROR: " + e.getMessage());
          e.printStackTrace();
          System.out.println(hLine);
        }
      }
    } catch (Exception e) {
      System.out.println("ERROR: " + e.getMessage());
    } finally {
      tajoProxyClient.close();
    }
  }

  public void listUsers() throws Exception {
    System.out.println(hLine);
    List<ProxyUser> users = tajoProxyClient.listProxyUsers();
    if (users.isEmpty()) {
      System.out.println("No users.");
    } else {
      for (ProxyUser eachUser : users) {
        System.out.println(eachUser.toFormatString());
      }
    }
    System.out.println(hLine);
  }

  public void addUserToGroup() throws Exception {
    System.out.println(hLine);
    try {
      String groupName = reader.readLine("Group Name: ");
      if (groupName == null || groupName.trim().isEmpty()) {
        return;
      }
      List<ProxyUser> users = tajoProxyClient.listProxyUsers();
      String userList = "";
      String prefix = "";
      for (ProxyUser eachUser : users) {
        userList += prefix + eachUser.getUserId();
        prefix = ", ";
      }

      String selectedUsers = reader.readLine("Select users [" + userList + "]: ");
      if (selectedUsers == null || selectedUsers.trim().isEmpty()) {
        System.out.println("Should be selected at least one user.");
        return;
      }
      tajoProxyClient.changeGroupUsers(groupName, TUtil.newList(selectedUsers.split(",")), false);
    } finally {
      System.out.println(hLine);
    }
  }

  public void removeUserFromGroup() throws Exception {
    System.out.println(hLine);
    try {
      String groupName = reader.readLine("Group Name: ");
      if (groupName == null || groupName.trim().isEmpty()) {
        return;
      }
      List<ProxyUser> users = tajoProxyClient.listProxyUsers();
      String userList = "";
      String prefix = "";
      for (ProxyUser eachUser : users) {
        userList += prefix + eachUser.getUserId();
        prefix = ", ";
      }

      String selectedUsers = reader.readLine("Select users [" + userList + "]: ");
      if (selectedUsers == null || selectedUsers.trim().isEmpty()) {
        System.out.println("Should be selected at least one user.");
        return;
      }
      tajoProxyClient.changeGroupUsers(groupName, TUtil.newList(selectedUsers.split(",")), true);
    } finally {
      System.out.println(hLine);
    }
  }

  public void addGroup() throws Exception {
    System.out.println(hLine);
    String groupName = reader.readLine("Group Name: ");
    if (groupName != null && !groupName.trim().isEmpty()) {
      tajoProxyClient.addGroup(groupName);
    }
    System.out.println(hLine);
  }

  public void deleteGroup() throws Exception {
    System.out.println(hLine);
    String groupName = reader.readLine("Group Name: ");
    if (groupName != null && !groupName.trim().isEmpty()) {
      tajoProxyClient.deleteGroup(groupName);
    }
    System.out.println(hLine);
  }

  public void addUser() throws Exception {
    System.out.println(hLine);
    String userId = reader.readLine("UserID: ");
    String password = getPassword(userId);
    if (userId == null || password == null) {
      return;
    }
    List<ProxyGroup> groups = tajoProxyClient.getUserGroups();
    String groupList = "";
    String prefix = "";
    for (ProxyGroup eachGroup: groups) {
      groupList += prefix + eachGroup.getGroupName();
      prefix = ", ";
    }

    reader.setExpandEvents(false);

    String selectedGroup = reader.readLine("Select user's group [" + groupList + "]: ");
    if (selectedGroup == null || selectedGroup.trim().isEmpty()) {
      System.out.println("Should be selected at least one group.");
      System.exit(0);
    }

    ProxyUser proxyUser = new ProxyUser();
    proxyUser.setUserId(userId);
    proxyUser.setPassword(password);

    List<ProxyGroup> selectedProxyGroups = new ArrayList<ProxyGroup>();
    for (String eachGroupName: selectedGroup.split(",")) {
      ProxyGroup group = new ProxyGroup();
      group.setGroupName(eachGroupName);
      selectedProxyGroups.add(group);
    }
    proxyUser.setGroups(selectedProxyGroups);
    tajoProxyClient.addUser(proxyUser);
    System.out.println(hLine);
  }

  public void delUser() throws Exception {
    System.out.println(hLine);
    String userId = reader.readLine("UserID: ");
    if (userId == null) {
      return;
    }
    ProxyUser proxyUser = new ProxyUser();
    proxyUser.setUserId(userId);

    tajoProxyClient.deleteUser(proxyUser);
    System.out.println(userId + " removed.");
    System.out.println(hLine);
  }

  public void passwordUser() throws Exception {
    String userId = reader.readLine("UserID: ");
    if (userId == null) {
      System.out.println("ERROR: Enter UserID!");
      return;
    }
    String password = getPassword(userId);
    if (password == null || password.isEmpty()) {
      System.out.println("ERROR: Enter password!");
      return;
    }

    tajoProxyClient.changePassword(userId, password);
    System.out.println("[" + userId + "] password changed.");
    System.out.println(hLine);
  }

  private String getPassword(String userId) throws Exception {
    ConsoleReader reader = new ConsoleReader(System.in, System.out);
    reader.setExpandEvents(false);

    String password = reader.readLine("Password for [" + userId + "]: ", new Character('*'));
    String confirmPassword = reader.readLine("Retype password for [" + userId + "]: ", new Character('*'));

    if (!password.equals(confirmPassword)) {
      System.out.println("Password mismatch.");
      return null;
    }

    return password;
  }

  public void listGroups() throws Exception {
    System.out.println(hLine);
    List<ProxyGroup> groups = tajoProxyClient.getUserGroups();
    if (groups.isEmpty()) {
      System.out.println("No groups.");
    } else {
      for (ProxyGroup eachGroup : groups) {
        System.out.println(eachGroup.toFormatString());
      }
    }
    System.out.println(hLine);
  }

  public void grantTables() throws Exception {
    System.out.println(hLine);
    String groupName = reader.readLine("Group: ");
    if (groupName == null) {
      return;
    }

    String tables = reader.readLine("Enter a list of tables to be granted(comma separated): ");
    if (tables == null) {
      return;
    }
    tajoProxyClient.grantTables(groupName, TUtil.newList(tables.split(",")), true);
    System.out.println(hLine);
  }

  public void revokeTables() throws Exception {
    System.out.println(hLine);
    String groupName = reader.readLine("Group: ");
    if (groupName == null) {
      return;
    }

    String tables = reader.readLine("Enter a list of tables to be revoked(, separated): ");
    if (tables == null) {
      return;
    }
    tajoProxyClient.grantTables(groupName, TUtil.newList(tables.split(",")), false);
    System.out.println(hLine);
  }

  public boolean loginAdmin() throws Exception {
    ConsoleReader reader = new ConsoleReader(System.in, System.out);
    reader.setExpandEvents(false);

    String password = reader.readLine("Enter admin password: ", new Character('*'));
    try {
      return tajoProxyClient.login("admin", password);
    } catch (Exception e) {
      return false;
    }
  }

  public static void main(String[] args) throws Exception {
    ProxyUserAdmin proxyManager = new ProxyUserAdmin();
    proxyManager.run();
  }

  public static String encodingPassword(String password) throws Exception {
    if (password == null || password.trim().isEmpty()) {
      throw new IOException("No password");
    }
    MessageDigest md = MessageDigest.getInstance("MD5");
    md.update(password.getBytes("UTF-8"));
    byte raw[] = md.digest();
    BASE64Encoder encoder = new BASE64Encoder();
    String result = encoder.encodeBuffer(raw);

    // last char is LF
    return result.substring(0, result.length() - 1);
  }
}
