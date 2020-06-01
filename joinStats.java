/*
 * A java Program to estimate the size of a 
 * resulting join on two tables in a database. 
 * 
 * The database system used is postgres.
 * 
 * Tables to be joined should be given
 * via command line arguments. 
 */

import java.sql.*;
import java.util.Properties;

// Calculates the estimated size of a table after a join
// Calculates the size of the actual join
// Calculates the estimated error 
public class joinStats {
	public static void main(String[] args) throws SQLException {
		String tabel1 = null; // first table of the join
		String tabel2 = null; // second table of the join
		
		// checks if command line arguments were given
		try { 
			tabel1 = args[0];
			tabel2 = args[1];
		}
		catch(Exception e) {
			System.out.println("Not enouph command line arguments given");
			System.exit(1);
		}
		
		Connection conn = null;
		
		/* 
		 * READ ME!!!!!!!!!!!
		 * Tries to make a connection to the data base
		 * url will have to be changed to the location of the database you are trying to connect to
		 * E.x.
		 * 		"jdbc:postgresql://localhost/[database name]"
		 * 
		 * To specify the user name to connect with change props.setProperty("user","will"); to props.setProperty("user","[your user name]");
		 * Next, change props.setProperty("password","password"); to props.setProperty("password","[your password]");
		 * This should connect you to the database
		 */
		try {
			String url = "jdbc:postgresql://localhost/university"; // change this to location of db you are trying to connect to
			Properties props = new Properties();
			props.setProperty("user","will"); // props.setProperty("user","[Your user name]");
			props.setProperty("password","password"); // props.setProperty("password","[Your password]");
			conn = DriverManager.getConnection(url, props);
		}
		catch(Exception e) {
			System.out.println("Connection Unsuccesful. Check url, username, and password given");
			System.exit(1);
		}
		
		DatabaseMetaData metaData = conn.getMetaData(); // gets database meta data
		
		ResultSet tabel2FK = metaData.getExportedKeys(null,  null, tabel2); // Forigen Key info for table 2
		ResultSet tabel1FK = metaData.getExportedKeys(null, null, tabel1); // Forigen key info for table 1
		
		Statement st1 = null; // SQL statement for getting size of table 2
		Statement st2 = null; // SQL statement for getting size of table 1
		ResultSet rs1 = null; // resulting relation after executing st1
		ResultSet rs2 = null; // resulting relation after executing st2
		
		// Trys to executes the statements on the specified tables
		try {
			st1 = conn.createStatement();
			rs1 = st1.executeQuery("select count(*) from " + tabel2);
			st2 = conn.createStatement();
			rs2 = st2.executeQuery("select count(*) from " + tabel1);
		}
		catch(Exception e) {
			System.out.println("Table names provided are not in the database");
			System.exit(1);
		}
		
		// Gets the Count from each relation from above
		rs1.next();
		int size1 = rs1.getInt(1);
		rs2.next();
		int size2 = rs2.getInt(1);
		
		int estimatedSize = 0; // calculated estimate of the relation after the join
		boolean calculated = false; // flag for when the calculation has been performed
		
		// loops through all foreign key info in table 2
		while(tabel2FK.next()) {
			// checks if table 1 is foreign table of table 2
			if(tabel2FK.getString("FKTABLE_NAME").equals(tabel1)){
				System.out.println("Case 1");
				estimatedSize = size2;
				calculated = true;
				break;
			}
		}
		
		if(!calculated) {
			// loops through table 1 foreign key info of table 1
			while(tabel1FK.next()) {
				// checks if table 2 is a foreign table of table 1
				if(tabel1FK.getString("FKTABLE_NAME").equals(tabel2)) {
					System.out.println("Case 2");
					estimatedSize = size1;
					calculated = true;
					break;
				}
			}
		}
		
		if(!calculated) {
			// gets columns from table 1 and table 2
			ResultSet tabel1Columns = metaData.getColumns(null, null, tabel1, null);
			ResultSet tabel2Columns = metaData.getColumns(null, null, tabel2, null);
			String columnName; // currant column of table 1
			while(tabel1Columns.next()) {
				columnName = tabel1Columns.getString("COLUMN_NAME"); // gets the name of the currant column in table 1
				tabel2Columns.beforeFirst(); // resets the iterator back the the beginning of the table 2 columns result set
				
				// loops through each column in table 2
				while(tabel2Columns.next()) {
					//checks if the currant column in table 1 is the same as the currant column in table 2
					//Thus, RnS = {columnName} and columnName is not a primary key of R or S
					if(tabel2Columns.getString("COLUMN_NAME").equals(columnName)) {
						System.out.println("Case 3");
						calculated = true;
						st1 = conn.createStatement();
						rs1 = st1.executeQuery("select count(DISTINCT " + columnName + ") from " + tabel2); // V(columnName, tabel2)
						rs1.next();
						int size11 = rs1.getInt(1);
						st2 = conn.createStatement();
						rs2 = st2.executeQuery("select count(DISTINCT " + columnName + ") from " + tabel1); // V(columnName, tabel1)
						rs2.next();
						int size22 = rs2.getInt(1);
						
						//select the min between (nr x ns)/V(columnName, tabel1) and (nr x ns)/V(columnName, tabel2)
						estimatedSize = Math.min((size1 * size2) / size11, (size1 * size2) / size22);
						break;
					}
				}
				if(calculated) {
					break;
				}
			}
			
			// RnS = null thus the estimated size is nr x ns
			if(!calculated) {
				System.out.println("Case 4");
				estimatedSize = size1 * size2;
			}
		}
		
		System.out.println("Estimated Join Size: " + estimatedSize);
		
		// performs the actual join between the two tables and gets the size of it
		Statement st = conn.createStatement();
		ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM (SELECT * FROM " + tabel1 + " natural join " + tabel2 + ") a");
		rs.next();
		System.out.println("Actual Join Size: " + rs.getInt(1));
		
		// calculates the estimation error.
		System.out.println("Estimation Error: " + (estimatedSize - rs.getInt(1)));
		
	}
}