import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.TreeSet;
import java.util.Vector;


//json.simple 1.1
// import org.json.simple.JSONObject;
// import org.json.simple.JSONArray;

// Alternate implementation of JSON modules.
import org.json.JSONObject;
import org.json.JSONArray;

public class GetData{
	
    static String prefix = "project3.";
	
    // You must use the following variable as the JDBC connection
    Connection oracleConnection = null;
	
    // You must refer to the following variables for the corresponding 
    // tables in your database

    String cityTableName = null;
    String userTableName = null;
    String friendsTableName = null;
    String currentCityTableName = null;
    String hometownCityTableName = null;
    String programTableName = null;
    String educationTableName = null;
    String eventTableName = null;
    String participantTableName = null;
    String albumTableName = null;
    String photoTableName = null;
    String coverPhotoTableName = null;
    String tagTableName = null;

    // This is the data structure to store all users' information
    // DO NOT change the name
    JSONArray users_info = new JSONArray();		// declare a new JSONArray

	
    // DO NOT modify this constructor
    public GetData(String u, Connection c) {
	super();
	String dataType = u;
	oracleConnection = c;
	// You will use the following tables in your Java code
	cityTableName = prefix+dataType+"_CITIES";
	userTableName = prefix+dataType+"_USERS";
	friendsTableName = prefix+dataType+"_FRIENDS";
	currentCityTableName = prefix+dataType+"_USER_CURRENT_CITIES";
	hometownCityTableName = prefix+dataType+"_USER_HOMETOWN_CITIES";
	programTableName = prefix+dataType+"_PROGRAMS";
	educationTableName = prefix+dataType+"_EDUCATION";
	eventTableName = prefix+dataType+"_USER_EVENTS";
	albumTableName = prefix+dataType+"_ALBUMS";
	photoTableName = prefix+dataType+"_PHOTOS";
	tagTableName = prefix+dataType+"_TAGS";
    }
	

    //implement this function
    @SuppressWarnings("unchecked")
    public JSONArray toJSON() throws SQLException{
    	JSONArray users_info = new JSONArray();
	    // Your implementation goes here....
        try (Statement stmt = oracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)){
            ResultSet rst = stmt.executeQuery(
                    "SELECT * FROM " + userTableName
            );
            while(rst.next()){
                JSONObject user = new JSONObject();
                Long userId = rst.getLong(1);
                user.put("user_id", userId);
                user.put("first_name", rst.getString(2));
                user.put("last_name", rst.getString(3));
                user.put("YOB", rst.getInt(4));
                user.put("MOB", rst.getInt(5));
                user.put("DOB", rst.getInt(6));
                user.put("gender", rst.getString(7));
                Statement stmt2 = oracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
                ResultSet rst2 = stmt2.executeQuery(
                        "SELECT c.city_name, c.state_name, c.country_name " +
                                "FROM " + cityTableName + " c, " + hometownCityTableName + " h " +
                                "WHERE c.city_id = h.hometown_city_id AND h.user_id=" + userId
                );
                JSONObject hometown = new JSONObject();
                if (rst2.next()){
                    hometown.put("city", rst2.getString(1));
                    hometown.put("state", rst2.getString(2));
                    hometown.put("country", rst2.getString(3));
                }
                user.put("hometown", hometown); // empty if user does not have hometown

                rst2 = stmt2.executeQuery(
                        "SELECT c.city_name, c.state_name, c.country_name " +
                                "FROM " + cityTableName + " c, " + currentCityTableName + " h " +
                                "WHERE c.city_id = h.current_city_id AND h.user_id=" + userId
                );
                JSONObject current = new JSONObject();
                if (rst2.next()){
                    current.put("city", rst2.getString(1));
                    current.put("state", rst2.getString(2));
                    current.put("country", rst2.getString(3));
                }
                user.put("current", current);

                rst2 = stmt2.executeQuery(
                        "SELECT f.user2_id " +
                                "FROM " + friendsTableName + " f " +
                                "WHERE f.user1_id=" + userId
                );
                JSONArray friends = new JSONArray();
                while (rst2.next()){
                    friends.put(rst2.getLong(1));
                }
                user.put("friends", friends);

                users_info.put(user);
                rst2.close();
                stmt2.close();
            }
            rst.close();
        }
		
		return users_info;
    }

    // This outputs to a file "output.json"
    public void writeJSON(JSONArray users_info) {
	    // DO NOT MODIFY this function
        try {
            FileWriter file = new FileWriter(System.getProperty("user.dir")+"/output.json");
            file.write(users_info.toString());
            file.flush();
            file.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
