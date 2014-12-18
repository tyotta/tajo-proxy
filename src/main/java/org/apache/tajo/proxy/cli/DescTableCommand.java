package org.apache.tajo.proxy.cli;

import org.apache.commons.lang.CharUtils;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.TableDesc;
import org.apache.tajo.catalog.partition.PartitionMethodDesc;
import org.apache.tajo.cli.tsql.TajoCli;
import org.apache.tajo.cli.tsql.commands.TajoShellCommand;
import org.apache.tajo.proxy.cli.TajoProxyCli.TajoProxyCliContext;
import org.apache.tajo.util.FileUtil;
import org.apache.tajo.util.TUtil;

import java.util.List;
import java.util.Map;

public class DescTableCommand extends TajoProxyShellCommand {
  public DescTableCommand(TajoProxyCliContext context) {
    super(context);
  }

  @Override
  public String getCommand() {
    return "\\d";
  }

  @Override
  public void invoke(String[] cmd) throws Exception {
    if (cmd.length == 2) {
      String tableName = cmd[1];
      tableName = tableName.replace("\"", "");
      TableDesc desc = client.getTableDesc(tableName);
      if (desc == null) {
        context.getOutput().println("Did not find any relation named \"" + tableName + "\"");
      } else {
        context.getOutput().println(toFormattedString(desc));
      }
    } else if (cmd.length == 1) {
      List<String> tableList = client.getTableList(null);
      if (tableList.size() == 0) {
        context.getOutput().println("No Relation Found");
      }
      for (String table : tableList) {
        context.getOutput().println(table);
      }
    } else {
      throw new IllegalArgumentException();
    }
  }

  @Override
  public String getUsage() {
    return "[table_name]";
  }

  @Override
  public String getDescription() {
    return "show table description";
  }

  protected String toFormattedString(TableDesc desc) {
    StringBuilder sb = new StringBuilder();
    sb.append("\ntable name: ").append(desc.getName()).append("\n");
    sb.append("table path: ").append(desc.getPath()).append("\n");
    sb.append("store type: ").append(desc.getMeta().getStoreType()).append("\n");
    if (desc.getStats() != null) {
      sb.append("number of rows: ").append(desc.getStats().getNumRows()).append("\n");
      sb.append("volume: ").append(
          FileUtil.humanReadableByteCount(desc.getStats().getNumBytes(),
              true)).append("\n");
    }
    sb.append("Options: \n");
    for(Map.Entry<String, String> entry : desc.getMeta().toMap().entrySet()){

      /*
      *  Checks whether the character is ASCII 7 bit printable.
      *  For example, a printable unicode '\u007c' become the character ‘|’.
      *
      *  Control-chars : ctrl-a(\u0001), tab(\u0009) ..
      *  Printable-chars : '|'(\u007c), ','(\u002c) ..
      * */

      String value = entry.getValue();
      String unescaped = StringEscapeUtils.unescapeJava(value);
      if (unescaped.length() == 1 && CharUtils.isAsciiPrintable(unescaped.charAt(0))) {
        value = unescaped;
      }
      sb.append("\t").append("'").append(entry.getKey()).append("'").append("=")
          .append("'").append(value).append("'").append("\n");
    }
    sb.append("\n");
    sb.append("schema: \n");

    for(int i = 0; i < desc.getSchema().size(); i++) {
      Column col = desc.getSchema().getColumn(i);
      sb.append(col.getSimpleName()).append("\t").append(col.getDataType().getType());
      if (col.getDataType().hasLength()) {
        sb.append("(").append(col.getDataType().getLength()).append(")");
      }
      sb.append("\n");
    }

    sb.append("\n");
    if (desc.getPartitionMethod() != null) {
      PartitionMethodDesc partition = desc.getPartitionMethod();
      sb.append("Partitions: \n");

      sb.append("type:").append(partition.getPartitionType().name()).append("\n");

      sb.append("columns:").append(":");
      sb.append(TUtil.arrayToString(partition.getExpressionSchema().toArray()));
    }

    return sb.toString();
  }
}
