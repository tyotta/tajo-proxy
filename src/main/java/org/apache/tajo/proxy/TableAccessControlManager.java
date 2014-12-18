package org.apache.tajo.proxy;

import org.apache.tajo.algebra.*;
import org.apache.tajo.plan.algebra.BaseAlgebraVisitor;
import org.apache.tajo.plan.PlanningException;
import org.apache.tajo.ipc.ClientProtos.QueryRequest;
import org.apache.tajo.engine.parser.*;

import java.util.HashSet;
import java.util.Set;
import java.util.Stack;

public class TableAccessControlManager {
  private ProxyUserManageService userManager;
  public TableAccessControlManager(ProxyUserManageService userManager) {
    this.userManager = userManager;
  }

  public AccessControlContext checkTablePermission(String userCurrentDatabase, QueryRequest request) throws Exception {
    SQLAnalyzer analyzer = new SQLAnalyzer();
    Expr expr = analyzer.parse(request.getQuery());

    AccessControlContext context = new AccessControlContext();
    context.setCurrentDatabase(userCurrentDatabase);

    ProxyUser user = userManager.getSessionUser(request.getSessionId().getId());
    if (user == null) {
      context.setPermission(false);
      context.setErrorMessage("AccessControlContext: Not login user.");
      return context;
    }

    AccessControlVisitor visitor = new AccessControlVisitor();
    visitor.verifyAccessControl(context, expr);

    String notPermittedTables = "";
    String prefix = "";
    for (String eachTable: context.getTables()) {

      if (!userManager.hasPermission(user, eachTable)) {
        notPermittedTables += prefix + eachTable;
        context.setPermission(false);
        prefix = ",";
      }
    }

    if (!context.isPermission() && !notPermittedTables.isEmpty()) {
      context.setErrorMessage(notPermittedTables + " does not exists.");
    }
     return context;
  }

  public static class AccessControlContext {
    private String currentDatabase;
    private String errorMessage;
    private boolean permission = true;
    private Set<String> tables = new HashSet<String>();

    public String getCurrentDatabase() {
      return currentDatabase;
    }

    public void setCurrentDatabase(String currentDatabase) {
      this.currentDatabase = currentDatabase;
    }

    public String getErrorMessage() {
      return errorMessage;
    }

    public void setErrorMessage(String errorMessage) {
      this.errorMessage = errorMessage;
    }

    public boolean isPermission() {
      return permission;
    }

    public void setPermission(boolean permission) {
      this.permission = permission;
    }

    public Set<String> getTables() {
      return tables;
    }

    public void addTable(String table) {
      this.tables.add(table);
    }
  }

  public static class AccessControlVisitor extends BaseAlgebraVisitor<AccessControlContext, Expr> {
    public void verifyAccessControl(AccessControlContext context, Expr expr) throws PlanningException {
      visit(context, new Stack<Expr>(), expr);
    }

    @Override
    public Expr visitRelationList(AccessControlContext ctx,
                                 Stack<Expr> stack, RelationList expr) throws PlanningException {
      stack.push(expr);
      Expr child = null;
      for (Expr e : expr.getRelations()) {
        child = visit(ctx, stack, e);
      }
      stack.pop();
      return child;
    }

    @Override
    public Expr visitRelation(AccessControlContext ctx, Stack<Expr> stack, Relation expr) throws PlanningException {
      String tableName = expr.getName();
      if (tableName.indexOf(".") < 0) {
        ctx.addTable(ctx.getCurrentDatabase() + "." + expr.getName());
      } else {
        ctx.addTable(expr.getName());
      }
      return super.visitRelation(ctx, stack, expr);
     }
    @Override
    public Expr visitCreateDatabase(AccessControlContext ctx, Stack<Expr> stack, CreateDatabase expr)
        throws PlanningException {
      ctx.setPermission(false);
      ctx.setErrorMessage("Don't have 'Create Database' permission");
      return super.visitCreateDatabase(ctx, stack, expr);
    }
    public Expr visitDropDatabase(AccessControlContext ctx, Stack<Expr> stack, DropDatabase expr)
        throws PlanningException {
      ctx.setPermission(false);
      ctx.setErrorMessage("Don't have 'Drop Database' permission");
      return super.visitDropDatabase(ctx, stack, expr);
    }
    public Expr visitCreateTable(AccessControlContext ctx, Stack<Expr> stack, CreateTable expr) throws PlanningException {
      ctx.setPermission(false);
      ctx.setErrorMessage("Don't have 'Create Table' permission");
      return super.visitCreateTable(ctx, stack, expr);
    }
    public Expr visitDropTable(AccessControlContext ctx, Stack<Expr> stack, DropTable expr) throws PlanningException {
      ctx.setPermission(false);
      ctx.setErrorMessage("Don't have 'Drop Table' permission");
      return super.visitDropTable(ctx, stack, expr);
    }
    public Expr visitAlterTablespace(AccessControlContext ctx, Stack<Expr> stack, AlterTablespace expr) throws PlanningException {
      ctx.setPermission(false);
      ctx.setErrorMessage("Don't have 'Alter Tablespace' permission");
      return super.visitAlterTablespace(ctx, stack, expr);
    }
    public Expr visitAlterTable(AccessControlContext ctx, Stack<Expr> stack, AlterTable expr) throws PlanningException {
      ctx.setPermission(false);
      ctx.setErrorMessage("Don't have 'Alter Table' permission");
      return super.visitAlterTable(ctx, stack, expr);
    }
    public Expr visitTruncateTable(AccessControlContext ctx, Stack<Expr> stack, TruncateTable expr) throws PlanningException {
      ctx.setPermission(false);
      ctx.setErrorMessage("Don't have 'Truncate Table' permission");
      return super.visitTruncateTable(ctx, stack, expr);
    }

    // Insert or Update
    public Expr visitInsert(AccessControlContext ctx, Stack<Expr> stack, Insert expr) throws PlanningException {
      ctx.setPermission(false);
      ctx.setErrorMessage("Don't have 'Insert' permission");
      return super.visitInsert(ctx, stack, expr);
    }
  }

  public static void main(String[] args) throws Exception {
    SQLAnalyzer analyzer = new SQLAnalyzer();
    Expr expr = analyzer.parse("select * from a.table1 " +
        "join b.table2 on table1.a = table2.b " +
        "join (select * from table3 k) t3 on tabl2.a = t3.b");

    AccessControlVisitor visitor = new AccessControlVisitor();

    AccessControlContext ctx = new AccessControlContext();
    ctx.setCurrentDatabase("proxy");

    visitor.verifyAccessControl(ctx, expr);

    System.out.println(ctx.isPermission() + "," + ctx.getErrorMessage());

    for (String table: ctx.getTables()) {
      System.out.println(">>>>>>>" + table);
    }
  }
}
