package org.apache.tajo.jdbc.proxy;

import java.sql.*;

public class JdbcTest1 {
	public static void main(String[] args) {
		try {
			StringBuffer sSQL = new StringBuffer();

			Class.forName("org.apache.tajo.jdbc.proxy.TajoDriver");

			java.util.Properties props = new java.util.Properties();

			Connection con = null;
			Statement stmt = null;
			ResultSet rs = null;

			props.put("user", "admin"); //
			props.put("password", "admin"); //
			con = DriverManager.getConnection(
					"jdbc:tajo-proxy://127.0.0.1:27000/default?characterEncoding=EUC-KR", props);
																				
			DatabaseMetaData DBMetaData = con.getMetaData();

			DatabaseMetaData meta = con.getMetaData();
			System.out
					.println("==================================================================");
			System.out.println("Database Product Name    is ... "
					+ meta.getDatabaseProductName());
			System.out.println("Database Product Version is ... "
					+ meta.getDatabaseProductVersion());
			System.out.println("JDBC Driver Name is ........... "
					+ meta.getDriverName());
			System.out.println("JDBC Driver Version is ........ "
					+ meta.getDriverVersion());
			System.out.println("JDBC Driver Major Version is .. "
					+ meta.getDriverMajorVersion());
			System.out.println("JDBC Driver Minor Version is .. "
					+ meta.getDriverMinorVersion());
			System.out
					.println("==================================================================");

			System.out.println("\n\n");
			System.out.println("==============  query  ==============");
			// con.setAutoCommit(true); 
			stmt = con.createStatement();

			System.out.println("\n\n");
			System.out.println("==============  Catalog  ==============");
			rs = DBMetaData.getCatalogs();
			printResultSet(rs);

			// System.out.println("\n\n");

			System.out.println("==============  Schema  ==============");
			rs = DBMetaData.getSchemas(); // -- error
			printResultSet(rs);

			System.out.println("\n\n");

			System.out.println("==============  Table(all)  ==============");
			String[] type2 = { "TABLE", "VIEW" };
			rs = DBMetaData.getTables("default", null, null, type2);
			printResultSet(rs);

			System.out.println("\n\n");
			// //
			// System.out.println("==============  Table(type01)  ==============");
			// // rs = DBMetaData.getTables ("SDPM", null, "tc_sdp_sex_cd",
			// type2); // 
			// // printResultSet(rs);
			// //
			// // System.out.println("\n\n");
			// //
			// System.out.println("==============  Table(type02)  ==============");
			// // rs = DBMetaData.getTables ("SDPM", null, "TC_SDP_SEX_CD",
			// type2); // 
			// // printResultSet(rs);
			// //
			// // //
			// // System.out.println("\n\n");
			// // System.out.println("==============  Column  ==============");
			// // //rs = DBMetaData.getColumns("SDPM", null, "tc_sdp_sex_cd",
			// null); // 
			// // rs = DBMetaData.getColumns("SDPM", null,
			// "TM_S_SDP_MTHLY_APP_RPTS", null); //
			// // printResultSet(rs);

			System.out.println("\n\n---------------------------------------- 4");
			sSQL.setLength(0);
			sSQL.append(" SELECT * from table101 where id > 1 ");
//			sSQL.append(" SELECT * from table11");

			System.out.println(sSQL.toString() + "\n");
			boolean result = stmt.execute(sSQL.toString());
			if (result == true) {
				System.out.println("select");
				rs = stmt.getResultSet();
				printResultSet(rs);
			} else {
				System.out.println("update count: " + stmt.getUpdateCount());
			}

			con.close();
		} catch (Exception ex) {
			ex.printStackTrace();
			System.exit(-1);
		}
	}

	private static void printResultSet(ResultSet rs) throws Exception {
		ResultSetMetaData rsmd = rs.getMetaData();
		int numCols = rsmd.getColumnCount();

		System.out.println("numCols = " + numCols);

		String[] colNames = new String[numCols];
		String value;
		int[] colDisplaySizes = new int[numCols];
		int[] colType = new int[numCols];
		int totalDisplaySize = 1;

		for (int i = 0; i < numCols; i++) {
			colNames[i] = rsmd.getColumnName(i + 1);
			colDisplaySizes[i] = Math.max(rsmd.getColumnDisplaySize(i + 1),
					colNames[i].getBytes().length);
			if (colDisplaySizes[i] > 1024)
				colDisplaySizes[i] = colNames[i].getBytes().length + 3;
			else
				totalDisplaySize += colDisplaySizes[i] + 3;
			colType[i] = rsmd.getColumnType(i + 1);

			System.out.print(i + ": Name:" + colNames[i] + " - Type:"
					+ rsmd.getColumnTypeName(i + 1) + " - ");
			/*
			 * switch(colType[i]) { case Types.ARRAY :
			 * System.out.print(":  ARRAY        "); break; case Types.BIGINT :
			 * System.out.print(":  BIGINT       "); break; case Types.BINARY :
			 * System.out.print(":  BINARY       "); break; case Types.BIT :
			 * System.out.print(":  BIT          "); break; case Types.BLOB :
			 * System.out.print(":  BLOB         "); break; case Types.CHAR :
			 * System.out.print(":  CHAR         "); break; case Types.CLOB :
			 * System.out.print(":  CLOB         "); break; case Types.DATE :
			 * System.out.print(":  DATE         "); break; case Types.DECIMAL :
			 * System.out.print(":  DECIMAL      "); break; case Types.DISTINCT
			 * : System.out.print(":  DISTINCT     "); break; case Types.DOUBLE
			 * : System.out.print(":  DOUBLE       "); break; case Types.FLOAT :
			 * System.out.print(":  FLOAT        "); break; case Types.INTEGER :
			 * System.out.print(":  INTEGER      "); break; case
			 * Types.JAVA_OBJECT : System.out.print(":  JAVA_OBJECT  "); break;
			 * case Types.LONGVARBINARY : System.out.print(":  LONGVARBINARY");
			 * break; case Types.LONGVARCHAR :
			 * System.out.print(":  LONGVARCHAR  "); break; case Types.NULL :
			 * System.out.print(":  NULL         "); break; case Types.NUMERIC :
			 * System.out.print(":  NUMERIC      "); break; case Types.OTHER :
			 * System.out.print(":  OTHER        "); break; case Types.REAL :
			 * System.out.print(":  REAL         "); break; case Types.REF :
			 * System.out.print(":  REF          "); break; case Types.SMALLINT
			 * : System.out.print(":  SMALLINT     "); break; case Types.STRUCT
			 * : System.out.print(":  STRUCT       "); break; case Types.TIME :
			 * System.out.print(":  TIME         "); break; case Types.TIMESTAMP
			 * : System.out.print(":  TIMESTAMP    "); break; case Types.TINYINT
			 * : System.out.print(":  TINYINT      "); break; case
			 * Types.VARBINARY : System.out.print(":  VARBINARY    "); break;
			 * case Types.VARCHAR : System.out.print(":  VARCHAR      "); break;
			 * default : System.out.print(":  VARCHAR      "); break; }
			 */
			System.out.println(" - DisplaySize: " + colDisplaySizes[i] + " -- "
					+ rsmd.getColumnDisplaySize(i + 1) + "."
					+ rsmd.getScale(i + 1));
		}

		System.out.println(getLine('-', totalDisplaySize));
		System.out.print("|");
		for (int i = 0; i < numCols; i++) {
			System.out.print(" ");
			print(colNames[i], colDisplaySizes[i]);
			System.out.print(" |");
		}
		System.out.println();
		System.out.println(getLine('-', totalDisplaySize));

		int exceedcount = 0;
		byte[] colval;
		char ch1, ch2;
		int iLoop = 0;
		// while (rs.next())
		while (rs.next() && iLoop <= 10) {
			System.out.print("|");
			for (int i = 0; i < numCols; i++) {
				// if(i != 2)
				// continue;

				switch (colType[i]) {
				case Types.NUMERIC:
					print(rs.getString(i + 1), colDisplaySizes[i]);
					break;

				case Types.DOUBLE:
				case Types.FLOAT:
				case Types.REAL:
					print(Double.toString(rs.getDouble(i + 1)),
							colDisplaySizes[i]);
					print("  ", 10);
					break;

				default:
					print(rs.getString(i + 1), colDisplaySizes[i]);

					/*
					 * default : value = rs.getString(i+1); ch1 =
					 * value.charAt(0); ch2 = value.charAt(1);
					 * System.out.print(ch1); System.out.print(value.charAt(0));
					 * colval = value.getBytes(); colval =
					 * value.getBytes("8859_1"); colval =
					 * value.getBytes("euc_kr"); colval =
					 * value.getBytes("MS949"); value = new
					 * String(value.getBytes(), "euc_kr"); System.out.print(new
					 * String(value.getBytes(), "8859_1")); System.out.print(new
					 * String(value.getBytes("euc_kr"), "8859_1"));
					 * System.out.print(new String(value.getBytes("8859_1"),
					 * "euc_kr")); System.out.print(new
					 * String(value.getBytes("MS949"), "8859_1"));
					 * System.out.print(new String(value.getBytes("8859_1"),
					 * "MS949"));
					 */
					break;

				}

				System.out.print(" |");
			}
			System.out.println();

			// if(exceedcount++ > 2)
			// break;

			iLoop = iLoop + 1;
		}
		rs.close();
	}

	private static String getLine(char ch, int size) {
		char[] line = new char[size];
		for (int i = 0; i < size; ++i)
			line[i] = ch;
		return new String(line);
	}

	private static void print(String str, int size) {
		if (str == null)
			str = "NULL";
		System.out.print(str);
		int strSize = str.getBytes().length;
		int padSize = size - strSize;
		if (padSize < 0)
			padSize = 0;
		System.out.print(getLine(' ', padSize));
	}
}
