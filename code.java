import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.sql.SQLTransientConnectionException;
import java.sql.Statement;

//java -cp ".:/usr/share/java/mariadb-java-client.jar:" code.java

public class code {


	public static void populateNaufalTables(String jdbcUrl, String databaseName){
		String line;
		String[] data;
		String insertQuery;
		PreparedStatement preparedStatement;
		Connection connection = null;
		Boolean skipFirstLine = false;

		try (BufferedReader br = new BufferedReader(new FileReader("38538245.csv"))) {
			connection = DriverManager.getConnection(jdbcUrl);
			Statement statement = connection.createStatement();

			String UseDatabase = "use " + databaseName;
		    statement.executeUpdate(UseDatabase);

			while ((line = br.readLine()) != null) {

				if (!skipFirstLine){
					skipFirstLine = true;
					continue;
				}
				data = line.split(",");

				try{
					// Adjust this switch to use the last but one element for table type detection, since the last element is 'Table' column which we want to ignore
					switch (data[data.length - 1]) {
						case "User":
							insertQuery = "INSERT INTO User(UserID, Email, Username, HasBio, DoB) VALUES (?, ?, ?, ?, ?)";
							preparedStatement = connection.prepareStatement(insertQuery); 
							preparedStatement.setInt(1, Integer.parseInt(data[1])); // UserID
							preparedStatement.setString(2, data[2]); // Email
							preparedStatement.setString(3, data[3]); // Username
							preparedStatement.setBoolean(4, Boolean.parseBoolean(data[4])); // HasBio
							preparedStatement.setDate(5, java.sql.Date.valueOf(data[5])); // DoB
							preparedStatement.executeUpdate();
							break;
						case "Tweet":
							insertQuery = "INSERT INTO Tweet(TweetID, UserID, Date, Time) VALUES (?, ?, ?, ?)";
							preparedStatement = connection.prepareStatement(insertQuery);
							preparedStatement.setInt(1, Integer.parseInt(data[6])); // TweetID
							preparedStatement.setInt(2, Integer.parseInt(data[1])); // UserID
							preparedStatement.setDate(3, java.sql.Date.valueOf(data[9])); // Date
							preparedStatement.setTime(4, java.sql.Time.valueOf(data[10])); // Time
							preparedStatement.executeUpdate();
							break;
						case "Like":    
							insertQuery = "INSERT INTO Likes(LikeID, TweetID, UserID, Date, Time) VALUES (?, ?, ?, ?, ?)";
							preparedStatement = connection.prepareStatement(insertQuery); 
							preparedStatement.setInt(1, Integer.parseInt(data[7])); // LikeID
							preparedStatement.setInt(2, Integer.parseInt(data[6])); // TweetID
							preparedStatement.setInt(3, Integer.parseInt(data[1])); // UserID for Like
							preparedStatement.setDate(4, java.sql.Date.valueOf(data[9])); // Date
							preparedStatement.setTime(5, java.sql.Time.valueOf(data[10])); // Time
							preparedStatement.executeUpdate();
							break;
						case "Retweet":   
							insertQuery = "INSERT INTO Retweet(RetweetID, TweetID, UserID, Date, Time) VALUES (?, ?, ?, ?, ?)";
							preparedStatement = connection.prepareStatement(insertQuery);
							preparedStatement.setInt(1, Integer.parseInt(data[8])); // RetweetID
							preparedStatement.setInt(2, Integer.parseInt(data[6])); // TweetID
							preparedStatement.setInt(3, Integer.parseInt(data[1])); // UserID for Retweet
							preparedStatement.setDate(4, java.sql.Date.valueOf(data[9])); // Date
							preparedStatement.setTime(5, java.sql.Time.valueOf(data[10])); // Time
							preparedStatement.executeUpdate();
							break;
						default:
							System.out.println("Unknown table type: " + data[data.length - 1]);
					}
				} catch(SQLIntegrityConstraintViolationException e){/* ignore */}
			}

				} catch (IOException | SQLException e) {
					e.printStackTrace();
				}
				finally {
					if (connection != null) {
						try {
							connection.close();
						} 
						catch (SQLException e) { /* Ignored */}
					}
				}	


		System.out.println("Database for student 38538245 has been populated\n");
	}

    public static void runQuery(String jdbcUrl, String databaseName, int i, String query){

        // Establishing a connection to the MySQL server
		Connection connection = null;

		// Creating a Statement object for executing SQL commands
		try
		{
            connection = DriverManager.getConnection(jdbcUrl);
            Statement statement = connection.createStatement();
			
		    String UseDatabase = "use " + databaseName;
		    statement.executeUpdate(UseDatabase);
            
		    //run query
			try{
				ResultSet qRes = statement.executeQuery(query);//Result set is created to keep the table...

				if(qRes.next()==false){}
				else//process the result set.
				{
					String address = qRes.getString(1);
					System.out.print(address);
					while(qRes.next())
					{
						address = qRes.getString(1);
						System.out.print(", " + address);
						
					}
				}
			} catch(SQLIntegrityConstraintViolationException e){
				System.out.println("Query " + i + " - Delete not permitted, Foreign Key constraint breached\n");
			}
        }
		catch (SQLException e) 
		{
			e.printStackTrace();
		}
		finally {
		    if (connection != null) {
		        try {
		        	connection.close();
		        } 
		        catch (SQLException e) { /* Ignored */}
		    }
		}	
    }

	public static void runNaufalDB()
    {
        // JDBC connection parameters
        String jdbcUrl = "jdbc:mysql://localhost:3306/";
        Connection connection = null;

        // Database name to be created
        String databaseName = "NaufalDB";

        try {
            // Establishing a connection to the MySQL server
            connection = DriverManager.getConnection(jdbcUrl);

            // Creating a Statement object for executing SQL commands
            Statement statement = connection.createStatement();

			try{
				String makeDatabase = "CREATE DATABASE " + databaseName;
				statement.executeUpdate(makeDatabase);
			} catch(SQLTransientConnectionException e) {/*ignore */}

            String UseDatabase = "use " + databaseName;
		    statement.executeUpdate(UseDatabase);

            String createUserTable = "CREATE TABLE IF NOT EXISTS User(UserID INT PRIMARY KEY,Email VARCHAR(255) UNIQUE, Username VARCHAR(50) UNIQUE, HasBio BOOLEAN, DoB DATE)";

		    statement.executeUpdate(createUserTable);

		    String createTweetTable = "CREATE TABLE IF NOT EXISTS Tweet (TweetID INT PRIMARY KEY, UserID INT, Date DATE, Time TIME, FOREIGN KEY (UserID) REFERENCES User(UserID) ON DELETE RESTRICT)";

		    statement.executeUpdate(createTweetTable);

			String createLikeTable = "CREATE TABLE IF NOT EXISTS Likes (LikeID INT PRIMARY KEY, TweetID INT, UserID INT, Date DATE, Time TIME, FOREIGN KEY (TweetID) REFERENCES Tweet(TweetID) ON DELETE CASCADE, FOREIGN KEY (UserID) REFERENCES User(UserID) ON DELETE CASCADE)";

			statement.executeUpdate(createLikeTable);

			String createRetweetTable = "CREATE TABLE IF NOT EXISTS Retweet (RetweetID INT PRIMARY KEY, TweetID INT, UserID INT, Date DATE, Time TIME, FOREIGN KEY (TweetID) REFERENCES Tweet(TweetID) ON DELETE CASCADE, FOREIGN KEY (UserID) REFERENCES User(UserID) ON DELETE CASCADE)";

			statement.executeUpdate(createRetweetTable);



			System.out.println("\nDatabase for student 38538245 has been created\n");

        } catch (SQLException e) {
            e.printStackTrace();
        } finally
        {
            try{
        if(connection !=null){
         connection.close();
        }
        } catch (SQLException ex){
        System.out.println(ex.getMessage());
        }
        }

		//populates tables from CSV
		populateNaufalTables(jdbcUrl, databaseName);


        runQuery(jdbcUrl, databaseName, 1, "DELETE FROM User WHERE userID = 1;");
		runQuery(jdbcUrl, databaseName, 2, "DELETE FROM User WHERE userID = 5;");
		System.out.print("Query 3 - TweetID of tweet that has more than 3 likes: ");
		runQuery(jdbcUrl, databaseName, 3, "SELECT TweetID, COUNT(*) AS LikesCount FROM Likes GROUP BY TweetID HAVING COUNT(*) >3;");
		System.out.print("\n\nQuery 4 - UserID of user that have more than 3 tweet: ");
		runQuery(jdbcUrl, databaseName, 4, "SELECT UserID, COUNT(*) AS TweetCount FROM Tweet GROUP BY UserID HAVING COUNT(*) >3;");
		System.out.println("\n");

		try{            
			// Establishing a connection to the MySQL server
            connection = DriverManager.getConnection(jdbcUrl);

            // Creating a Statement object for executing SQL commands
            Statement statement = connection.createStatement();

            String dropDatabase = "drop database " + databaseName;
		    statement.executeUpdate(dropDatabase);

		} catch (SQLException e) {
			e.printStackTrace();
		} finally
		{
			try{
		if(connection !=null){
		connection.close();
		} 
		} catch (SQLException ex){
		System.out.println(ex.getMessage());
		}
		}
	}


    public static void main(String[] args) {
		
		runNaufalDB();

    }
}