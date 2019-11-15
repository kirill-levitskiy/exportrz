package com.export;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.json.JSONArray;
import java.io.File;
import java.io.FileOutputStream;

/**
 * Oracle JDBC Query Execution
 * Export into JSON file
 */

public class ExportRz {

  public static void main(String[] args) throws Exception {

    String dbUrl = "jdbc:oracle:thin:@";
    String dbUser = "";
    String dbPassword = "";
    String sqlQuery =
          "SELECT "
        + "CITY_ID, STREET_ID, STREET_UA, STREET_EN, "
        + "STREETTYPE_UA, STREETTYPE_EN, HOUSENUMBER_UA, POSTINDEX "
        + "FROM V_EXPORT_RZ "
        + "WHERE ROWNUM <= 10";
    // ^ Remove WHERE Clause to retrieve all dataset ^

    System.out.println("-------- Oracle JDBC Connectivity  ------");

    try {
      // Returns the Class object associated with the class
      Class.forName("oracle.jdbc.driver.OracleDriver");
    } catch (ClassNotFoundException exception) {
      System.out.println("Oracle Driver Class Not found Exception: " + exception.toString());
      return;
    }

    // Set connection timeout
    DriverManager.setLoginTimeout(5);
    System.out.println("Oracle JDBC Driver Successfully Registered! Let's make connection now");

    Connection conn = null;
    try {
      // Attempts to establish a connection
      conn = DriverManager.getConnection(dbUrl, dbUser, dbPassword);
    } catch (SQLException e) {
      System.out.println("Connection Failed! Check output console");
      e.printStackTrace();
      return;
    }

    // Creates a Statement object for sending SQL statements to the database
    Statement stmt = conn.createStatement();

    // Executes the given SQL statement, which returns a single ResultSet object
    ResultSet resultset = stmt.executeQuery(
        sqlQuery);

    JSONArray jsonArray = new JsonConverter().convertToJSON(resultset);

    System.out.println("JSON length = " + jsonArray.length());


    if (writeFile(new File("jsonOutput.json"), jsonArray.toString() )) {
      System.out.println("Oracle JDBC connect and write file completed.");
    } else {
      throw new SQLException("Can NOT write file");
    }

  }

  private static boolean writeFile(File file, String conts){
    try {
      file.createNewFile();
      FileOutputStream fOut = new FileOutputStream(file);
      OutputStreamWriter outWriter = new OutputStreamWriter(fOut);
      outWriter.append(conts);
      outWriter.close();
      fOut.flush();
      fOut.close();
      return(true);
    } catch(IOException e) {
      return(false);
    }
  }

}
