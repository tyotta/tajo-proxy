package org.apache.tajo.proxy.cli;

import org.apache.tajo.catalog.CatalogUtil;
import org.apache.tajo.catalog.FunctionDesc;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.cli.tsql.TajoCli;
import org.apache.tajo.cli.tsql.commands.TajoShellCommand;
import org.apache.tajo.proxy.cli.TajoProxyCli.TajoProxyCliContext;
import org.apache.tajo.function.FunctionUtil;

import java.util.*;

import static org.apache.tajo.common.TajoDataTypes.DataType;

public class DescFunctionCommand extends TajoProxyShellCommand {
  public DescFunctionCommand(TajoProxyCliContext context) {
    super(context);
  }

  @Override
  public String getCommand() {
    return "\\df";
  }

  @Override
  public void invoke(String[] cmd) throws Exception {
    boolean printDetail = false;
    String functionName = "";
    if(cmd.length == 0) {
      throw new IllegalArgumentException();
    }

    if (cmd.length == 2) {
      printDetail = true;
      functionName = cmd[1];
    }

    List<CatalogProtos.FunctionDescProto> functions =
        new ArrayList<CatalogProtos.FunctionDescProto>(client.getFunctions(functionName));

    Collections.sort(functions, new Comparator<CatalogProtos.FunctionDescProto>() {
      @Override
      public int compare(CatalogProtos.FunctionDescProto f1, CatalogProtos.FunctionDescProto f2) {
        int nameCompared = f1.getSignature().compareTo(f2.getSignature());
        if (nameCompared != 0) {
          return nameCompared;
        } else {
          return f1.getReturnType().getType().compareTo(f2.getReturnType().getType());
        }
      }
    });

    String[] headers = new String[]{"Name", "Result type", "Argument types", "Description", "Type"};
    float[] columnWidthRates = new float[]{0.15f, 0.15f, 0.2f, 0.4f, 0.1f};
    int[] columnWidths = printHeader(headers, columnWidthRates);

    for(CatalogProtos.FunctionDescProto eachFunction: functions) {
      String name = eachFunction.getSignature();
      String resultDataType = eachFunction.getReturnType().getType().toString();
      String arguments = FunctionUtil.buildParamTypeString(
          eachFunction.getParameterTypesList().toArray(
              new DataType[eachFunction.getParameterTypesCount()]));
      String functionType = eachFunction.getType().toString();
      String description = eachFunction.getDescription();

      int index = 0;
      printLeft(" " + name, columnWidths[index++]);
      context.getOutput().print("|");
      printLeft(" " + resultDataType, columnWidths[index++]);
      context.getOutput().print("|");
      printLeft(" " + arguments, columnWidths[index++]);
      context.getOutput().print("|");
      printLeft(" " + description, columnWidths[index++]);
      context.getOutput().print("|");
      printLeft(" " + functionType, columnWidths[index++]);

      println();
    }

    println();
    context.getOutput().println("(" + functions.size() + ") rows");
    println();

    if (printDetail && !functions.isEmpty()) {
      Map<String, CatalogProtos.FunctionDescProto> functionMap =
          new HashMap<String, CatalogProtos.FunctionDescProto>();

      for (CatalogProtos.FunctionDescProto eachFunction: functions) {
        if (!functionMap.containsKey(eachFunction.getDescription())) {
          functionMap.put(eachFunction.getDescription(), eachFunction);
        }
      }

      for (CatalogProtos.FunctionDescProto eachFunction: functionMap.values()) {
        String signature = eachFunction.getReturnType().getType() + " " +
            FunctionUtil.buildSimpleFunctionSignature(eachFunction.getSignature(), eachFunction.getParameterTypesList());
        String fullDescription = eachFunction.getDescription();
        if(eachFunction.getDetail() != null && !eachFunction.getDetail().isEmpty()) {
          fullDescription += "\n" + eachFunction.getDetail();
        }

        context.getOutput().println("Function:    " + signature);
        context.getOutput().println("Description: " + fullDescription);
        context.getOutput().println("Example:\n" + eachFunction.getExample());
        println();
      }
    }
  }

  @Override
  public String getUsage() {
    return "[function_name]";
  }

  @Override
  public String getDescription() {
    return "show function description";
  }


}
