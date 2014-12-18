package org.apache.tajo.proxy.cli;

import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.jdbc.proxy.TajoProxyClient;
import org.apache.tajo.proxy.cli.TajoProxyCli.TajoProxyCliContext;

public abstract class TajoProxyShellCommand {
  public abstract String getCommand();
  public String [] getAliases() {
    return new String[] {};
  }
  public abstract void invoke(String [] command) throws Exception;
  public abstract String getUsage();
  public abstract String getDescription();
  public void printHelp() {
    context.getOutput().print(getCommand());
    context.getOutput().print(" - ");
    context.getOutput().println(getDescription());
  }

  protected TajoProxyCliContext context;
  protected TajoProxyClient client;
  protected int maxColumn;

  public TajoProxyShellCommand(TajoProxyCliContext context) {
    maxColumn = context.getConf().getIntVar(TajoConf.ConfVars.$CLI_MAX_COLUMN);
    this.context = context;
    client = context.getTajoProxyClient();
  }

  protected void println() {
    context.getOutput().println();
  }

  protected void printLeft(String message, int columnWidth) {
    int messageLength = message.length();

    if(messageLength >= columnWidth) {
      context.getOutput().print(message.substring(0, columnWidth - 1));
    } else {
      context.getOutput().print(message);
      print(' ', columnWidth - messageLength - 1);
    }
  }

  protected void printCenter(String message, int columnWidth, boolean warp) {
    int messageLength = message.length();

    if(messageLength > columnWidth) {
      context.getOutput().print(message.substring(0, columnWidth - 1));
    } else {
      int numPadding = (columnWidth - messageLength)/2;

      print(' ', numPadding);
      context.getOutput().print(message);
      print(' ', numPadding);
    }
    if(warp) {
      println();
    }
  }

  protected void printCenter(String message) {
    printCenter(message, maxColumn, true);
  }

  protected void print(char c, int count) {
    for(int i = 0; i < count; i++) {
      context.getOutput().print(c);
    }
  }

  protected int[] printHeader(String[] headers, float[] columnWidthRates) {
    int[] columnWidths = new int[columnWidthRates.length];

    int columnWidthSum = 0;
    for(int i = 0; i < columnWidths.length; i++) {
      columnWidths[i] = (int)(maxColumn * columnWidthRates[i]);
      if(i > 0) {
        columnWidthSum += columnWidths[i - 1];
      }
    }

    columnWidths[columnWidths.length - 1] = maxColumn - columnWidthSum;

    String prefix = "";
    for(int i = 0; i < headers.length; i++) {
      context.getOutput().print(prefix);
      printLeft(" " + headers[i], columnWidths[i]);
      prefix = "|";
    }
    println();

    int index = 0;
    int printPos = columnWidths[index] - 1;
    for(int i = 0; i < maxColumn; i++) {
      if(i == printPos) {
        if(index < columnWidths.length - 1) {
          print('+', 1);
          index++;
          printPos += columnWidths[index];
        }
      } else {
        print('-', 1);
      }
    }

    println();
    return columnWidths;
  }
}
