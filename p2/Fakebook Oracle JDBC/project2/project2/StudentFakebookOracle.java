package project2;

import javax.print.DocFlavor;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.ResultSet;
import java.util.ListIterator;

/*
    The StudentFakebookOracle class is derived from the FakebookOracle class and implements
    the abstract query functions that investigate the database provided via the <connection>
    parameter of the constructor to discover specific information.
*/
public final class StudentFakebookOracle extends FakebookOracle {
    // [Constructor]
    // REQUIRES: <connection> is a valid JDBC connection
    public StudentFakebookOracle(Connection connection) {
        oracle = connection;
    }
    
    @Override
    // Query 0
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find the total number of users for which a birth month is listed
    //        (B) Find the birth month in which the most users were born
    //        (C) Find the birth month in which the fewest users (at least one) were born
    //        (D) Find the IDs, first names, and last names of users born in the month
    //            identified in (B)
    //        (E) Find the IDs, first names, and last name of users born in the month
    //            identified in (C)
    //
    // This query is provided to you completed for reference. Below you will find the appropriate
    // mechanisms for opening up a statement, executing a query, walking through results, extracting
    // data, and more things that you will need to do for the remaining nine queries
    public BirthMonthInfo findMonthOfBirthInfo() throws SQLException {
        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll, FakebookOracleConstants.ReadOnly)) {
            // Step 1
            // ------------
            // * Find the total number of users with birth month info
            // * Find the month in which the most users were born
            // * Find the month in which the fewest (but at least 1) users were born
            ResultSet rst = stmt.executeQuery(
                "SELECT COUNT(*) AS Birthed, Month_of_Birth " +         // select birth months and number of uses with that birth month
                "FROM " + UsersTable + " " +                            // from all users
                "WHERE Month_of_Birth IS NOT NULL " +                   // for which a birth month is available
                "GROUP BY Month_of_Birth " +                            // group into buckets by birth month
                "ORDER BY Birthed DESC, Month_of_Birth ASC");           // sort by users born in that month, descending; break ties by birth month
            
            int mostMonth = 0;
            int leastMonth = 0;
            int total = 0;
            while (rst.next()) {                       // step through result rows/records one by one
                if (rst.isFirst()) {                   // if first record
                    mostMonth = rst.getInt(2);         //   it is the month with the most
                }
                if (rst.isLast()) {                    // if last record
                    leastMonth = rst.getInt(2);        //   it is the month with the least
                }
                total += rst.getInt(1);                // get the first field's value as an integer
            }
            BirthMonthInfo info = new BirthMonthInfo(total, mostMonth, leastMonth);
            
            // Step 2
            // ------------
            // * Get the names of users born in the most popular birth month
            rst = stmt.executeQuery(
                "SELECT User_ID, First_Name, Last_Name " +                // select ID, first name, and last name
                "FROM " + UsersTable + " " +                              // from all users
                "WHERE Month_of_Birth = " + mostMonth + " " +             // born in the most popular birth month
                "ORDER BY User_ID");                                      // sort smaller IDs first
                
            while (rst.next()) {
                info.addMostPopularBirthMonthUser(new UserInfo(rst.getLong(1), rst.getString(2), rst.getString(3)));
            }

            // Step 3
            // ------------
            // * Get the names of users born in the least popular birth month
            rst = stmt.executeQuery(
                "SELECT User_ID, First_Name, Last_Name " +                // select ID, first name, and last name
                "FROM " + UsersTable + " " +                              // from all users
                "WHERE Month_of_Birth = " + leastMonth + " " +            // born in the least popular birth month
                "ORDER BY User_ID");                                      // sort smaller IDs first
                
            while (rst.next()) {
                info.addLeastPopularBirthMonthUser(new UserInfo(rst.getLong(1), rst.getString(2), rst.getString(3)));
            }

            // Step 4
            // ------------
            // * Close resources being used
            rst.close();
            stmt.close();                            // if you close the statement first, the result set gets closed automatically

            return info;

        }
        catch (SQLException e) {
            System.err.println(e.getMessage());
            return new BirthMonthInfo(-1, -1, -1);
        }
    }

    @Override
    // Query 1
    // -----------------------------------------------------------------------------------
    // GOALS: (A) The first name(s) with the most letters
    //        (B) The first name(s) with the fewest letters
    //        (C) The first name held by the most users
    //        (D) The number of users whose first name is that identified in (C)
    public FirstNameInfo findNameInfo() throws SQLException {
        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll, FakebookOracleConstants.ReadOnly)) {
            /*
                EXAMPLE DATA STRUCTURE USAGE
                ============================================
                FirstNameInfo info = new FirstNameInfo();
                info.addLongName("Aristophanes");
                info.addLongName("Michelangelo");
                info.addLongName("Peisistratos");
                info.addShortName("Bob");
                info.addShortName("Sue");
                info.addCommonName("Harold");
                info.addCommonName("Jessica");
                info.setCommonNameCount(42);
                return info;
            */
            FirstNameInfo info = new FirstNameInfo();
            ResultSet rst = stmt.executeQuery(
                    "select distinct u.FIRST_NAME " +
                            "from " + UsersTable + " u " +
                            "where length(u.FIRST_NAME) = ( " +
                            "select max(length(u2.FIRST_NAME)) " +
                            "from " + UsersTable + " u2) " +
                            "order by u.FIRST_NAME");
            while (rst.next()) {
                info.addLongName(rst.getString(1));
            }

            rst = stmt.executeQuery(
                    "select distinct u.FIRST_NAME " +
                            "from " + UsersTable + " u " +
                            "where length(u.FIRST_NAME) = ( " +
                            "select min(length(u2.FIRST_NAME)) " +
                            "from " + UsersTable + " u2) " +
                            "order by u.FIRST_NAME");
            while (rst.next()){
                info.addShortName(rst.getString(1));
            }

            stmt.executeUpdate(
                    "create view name_times as " +
                            "select count (*) as times, u.FIRST_NAME " +
                            "from " + UsersTable + " u " +
                            "group by u.FIRST_NAME " +
                            "order by times desc, u.FIRST_NAME");

            rst = stmt.executeQuery("select max(nt.times) from name_times nt");
            int count = 0;
            while (rst.next()){
                count = rst.getInt(1);
            }
            info.setCommonNameCount(count);

            rst = stmt.executeQuery(
                    "select nt.FIRST_NAME " +
                            "from name_times nt " +
                            "where nt.times = " + count);
            while (rst.next()){
                info.addCommonName(rst.getString(1));
            }

            stmt.executeUpdate("drop view name_times");
            rst.close();
            stmt.close();
            return info;
        }
        catch (SQLException e) {
            System.out.println("ERROR");
            System.err.println(e.getMessage());
            return new FirstNameInfo();
        }
    }

    @Override
    // Query 2
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find the IDs, first names, and last names of users without any friends
    //
    // Be careful! Remember that if two users are friends, the Friends table only contains
    // the one entry (U1, U2) where U1 < U2.
    public FakebookArrayList<UserInfo> lonelyUsers() throws SQLException {
        FakebookArrayList<UserInfo> results = new FakebookArrayList<UserInfo>(", ");

        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll, FakebookOracleConstants.ReadOnly)) {
            /*
                EXAMPLE DATA STRUCTURE USAGE
                ============================================
                UserInfo u1 = new UserInfo(15, "Abraham", "Lincoln");
                UserInfo u2 = new UserInfo(39, "Margaret", "Thatcher");
                results.add(u1);
                results.add(u2);
            */
            ResultSet rst = stmt.executeQuery(
                    "select U.USER_ID, u.FIRST_NAME, u.LAST_NAME " +
                            "from " + UsersTable + " u " +
                            "where u.USER_ID not in (select f.USER1_ID from " + FriendsTable + " f) " +
                            "and u.USER_ID not in (select f.USER2_ID from " + FriendsTable + " f) " +
                            "order by u.USER_ID");
            while (rst.next()){
                UserInfo info = new UserInfo(rst.getLong(1), rst.getString(2), rst.getString(3));
                results.add(info);
            }
            rst.close();
        }
        catch (SQLException e) {
            System.out.println("ERROR");
            System.err.println(e.getMessage());
        }
        return results;
    }

    @Override
    // Query 3
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find the IDs, first names, and last names of users who no longer live
    //            in their hometown (i.e. their current city and their hometown are different)
    public FakebookArrayList<UserInfo> liveAwayFromHome() throws SQLException {
        FakebookArrayList<UserInfo> results = new FakebookArrayList<UserInfo>(", ");

        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll, FakebookOracleConstants.ReadOnly)) {
            /*
                EXAMPLE DATA STRUCTURE USAGE
                ============================================
                UserInfo u1 = new UserInfo(9, "Meryl", "Streep");
                UserInfo u2 = new UserInfo(104, "Tom", "Hanks");
                results.add(u1);
                results.add(u2);
            */
            stmt.executeUpdate(
                    "create view qualified_user as " +
                            "select u.USER_ID, ucc.CURRENT_CITY_ID as cur, uhc.HOMETOWN_CITY_ID as home " +
                            "from " + UsersTable + " u, " + CurrentCitiesTable + " ucc, " + HometownCitiesTable + " uhc " +
                            "where u.USER_ID = ucc.USER_ID and u.USER_ID = uhc.USER_ID " +
                            "minus " +
                            "select u.USER_ID, ucc.CURRENT_CITY_ID cur, ucc.CURRENT_CITY_ID as home " +
                            "from project2.PUBLIC_USERS u, project2.PUBLIC_USER_CURRENT_CITIES ucc " +
                            "where u.USER_ID = ucc.USER_ID");

            ResultSet rst = stmt.executeQuery(
                    "select u.USER_ID, u.FIRST_NAME, u.LAST_NAME " +
                            "from " + UsersTable + " u " +
                            "where u.USER_ID in ( " +
                            "select q.USER_ID " +
                            "from qualified_user q) " +
                            "order by u.USER_ID");

            while (rst.next()){
                UserInfo info = new UserInfo(rst.getLong(1), rst.getString(2), rst.getString(3));
                results.add(info);
            }

            stmt.executeUpdate("drop view qualified_user");
            rst.close();
        }
        catch (SQLException e) {
            System.out.println("ERROR");
            System.err.println(e.getMessage());
        }

        return results;
    }

    @Override
    // Query 4
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find the IDs, links, and IDs and names of the containing album of the top
    //            <num> photos with the most tagged users
    //        (B) For each photo identified in (A), find the IDs, first names, and last names
    //            of the users therein tagged
    public FakebookArrayList<TaggedPhotoInfo> findPhotosWithMostTags(int num) throws SQLException {
        FakebookArrayList<TaggedPhotoInfo> results = new FakebookArrayList<TaggedPhotoInfo>("\n");

        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll, FakebookOracleConstants.ReadOnly)) {
            /*
                EXAMPLE DATA STRUCTURE USAGE
                ============================================
                PhotoInfo p = new PhotoInfo(80, 5, "www.photolink.net", "Winterfell S1");
                UserInfo u1 = new UserInfo(3901, "Jon", "Snow");
                UserInfo u2 = new UserInfo(3902, "Arya", "Stark");
                UserInfo u3 = new UserInfo(3903, "Sansa", "Stark");
                TaggedPhotoInfo tp = new TaggedPhotoInfo(p);
                tp.addTaggedUser(u1);
                tp.addTaggedUser(u2);
                tp.addTaggedUser(u3);
                results.add(tp);
            */
            ResultSet rst = stmt.executeQuery(
                    "select selected_photo.PHOTO_ID, selected_photo.ALBUM_ID, selected_photo.PHOTO_LINK, selected_photo.ALBUM_NAME " +
                            "from  ( " +
                            "select count(*) as times, p.PHOTO_ID, p.PHOTO_LINK, p.ALBUM_ID, a.ALBUM_NAME " +
                            "from " + PhotosTable + " p, " + TagsTable + " t, " + AlbumsTable + " a " +
                            "where p.PHOTO_ID = t.TAG_PHOTO_ID and p.ALBUM_ID = a.ALBUM_ID " +
                            "group by p.PHOTO_ID, p.PHOTO_LINK, p.ALBUM_ID, a.ALBUM_NAME " +
                            "order by times desc, p.PHOTO_ID)selected_photo " +
                            "where ROWNUM <= " + num);

            Statement stmt2 = oracle.createStatement(FakebookOracleConstants.AllScroll, FakebookOracleConstants.ReadOnly);
            ResultSet sub_rst = null;
            while (rst.next()){
                PhotoInfo p = new PhotoInfo(rst.getLong(1), rst.getLong(2),
                        rst.getString(3), rst.getString(4));
                TaggedPhotoInfo tp = new TaggedPhotoInfo(p);
                sub_rst = stmt2.executeQuery(
                        "select u.USER_ID, u.FIRST_NAME, u.LAST_NAME " +
                                "from " + TagsTable + " t, " + UsersTable + " u " +
                                "where t.TAG_PHOTO_ID = " + rst.getLong(1) + " and t.TAG_SUBJECT_ID = u.USER_ID " +
                                "order by u.USER_ID");
                while (sub_rst.next()){
                    UserInfo u = new UserInfo(sub_rst.getLong(1), sub_rst.getString(2), sub_rst.getString(3));
                    tp.addTaggedUser(u);
                }
                results.add(tp);
            }

            sub_rst.close();
            stmt2.close();
            rst.close();
        }
        catch (SQLException e) {
            System.out.println("ERROR");
            System.err.println(e.getMessage());
        }
        return results;
    }

    @Override
    // Query 5
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find the IDs, first names, last names, and birth years of each of the two
    //            users in the top <num> pairs of users that meet each of the following
    //            criteria:
    //              (i) same gender
    //              (ii) tagged in at least one common photo
    //              (iii) difference in birth years is no more than <yearDiff>
    //              (iv) not friends
    //        (B) For each pair identified in (A), find the IDs, links, and IDs and names of
    //            the containing album of each photo in which they are tagged together
    public FakebookArrayList<MatchPair> matchMaker(int num, int yearDiff) throws SQLException {
        FakebookArrayList<MatchPair> results = new FakebookArrayList<MatchPair>("\n");

        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll, FakebookOracleConstants.ReadOnly)) {
            /*
                EXAMPLE DATA STRUCTURE USAGE
                ============================================
                UserInfo u1 = new UserInfo(93103, "Romeo", "Montague");
                UserInfo u2 = new UserInfo(93113, "Juliet", "Capulet");
                MatchPair mp = new MatchPair(u1, 1597, u2, 1597);
                PhotoInfo p = new PhotoInfo(167, 309, "www.photolink.net", "Tragedy");
                mp.addSharedPhoto(p);
                results.add(mp);
            */

            stmt.executeUpdate(
                    "create view selected as " +
                            "select distinct nf.USER1_ID, nf.USER2_ID, t1.TAG_PHOTO_ID " +
                            "from " + TagsTable + " t1, " + TagsTable + " t2, " +
                            "(select u1.USER_ID as USER1_ID, u2.USER_ID as USER2_ID " +
                            "from " + UsersTable + " u1, " + UsersTable + " u2 " +
                            "where u1.USER_ID < u2.USER_ID and u1.GENDER = u2.GENDER " +
                            "and abs(u1.YEAR_OF_BIRTH-u2.YEAR_OF_BIRTH) <= " + yearDiff +
                            " and (u1.USER_ID, u2.USER_ID) not in (select f.USER1_ID, f.USER2_ID from " + FriendsTable + " f)) nf " +
                            "where nf.USER1_ID = t1.TAG_SUBJECT_ID and nf.USER2_ID = t2.TAG_SUBJECT_ID " +
                            "and t1.TAG_PHOTO_ID = t2.TAG_PHOTO_ID");

            ResultSet rst = stmt.executeQuery(
                    "select u1.USER_ID, u2.USER_ID, u1.FIRST_NAME, u2.FIRST_NAME, " +
                            "u1.LAST_NAME, u2.LAST_NAME, u1.YEAR_OF_BIRTH, u2.YEAR_OF_BIRTH " +
                            "from " + UsersTable + " u1, " + UsersTable + " u2, " +
                            "(select temp.USER1_ID, temp.USER2_ID, temp.common_tags from " +
                            "(select s.USER1_ID, s.USER2_ID, count(*) as common_tags from selected s " +
                            "group by (s.USER1_ID, s.USER2_ID) " +
                            "order by common_tags) temp " +
                            "where ROWNUM <= " + num + ") sp " +
                            "where sp.USER1_ID = u1.USER_ID and sp.USER2_ID = u2.USER_ID " +
                            "order by sp.common_tags, u1.USER_ID, u2.USER_ID");

            Statement stmt2 = oracle.createStatement(FakebookOracleConstants.AllScroll, FakebookOracleConstants.ReadOnly);
            ResultSet sub_rst = null;
            while (rst.next()){
                UserInfo u1 = new UserInfo(rst.getLong(1), rst.getString(3), rst.getString(5));
                UserInfo u2 = new UserInfo(rst.getLong(2), rst.getString(4), rst.getString(6));
                MatchPair mp = new MatchPair(u1, rst.getLong(7), u2, rst.getLong(8));
                sub_rst = stmt2.executeQuery(
                        "select s.TAG_PHOTO_ID, p.ALBUM_ID, p.PHOTO_LINK, a.ALBUM_NAME " +
                                "from selected s, project2.PUBLIC_PHOTOS p, project2.PUBLIC_ALBUMS a " +
                                "where s.USER1_ID = " + rst.getLong(1) + " and s.USER2_ID = " + rst.getLong(2) + " and s.TAG_PHOTO_ID = p.PHOTO_ID " +
                                "and p.ALBUM_ID = a.ALBUM_ID " +
                                "order by s.TAG_PHOTO_ID");
                while (sub_rst.next()){
                    PhotoInfo p = new PhotoInfo(sub_rst.getLong(1), sub_rst.getLong(2),
                            sub_rst.getString(3), sub_rst.getString(4));
                    mp.addSharedPhoto(p);
                }
                results.add(mp);
            }

            stmt.executeUpdate("drop view selected");
            
            rst.close();
            sub_rst.close();
            stmt2.close();
        }
        catch (SQLException e) {
            System.out.println("ERROR");
            System.err.println(e.getMessage());
        }

        return results;
    }
    
    @Override
    // Query 6
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find the IDs, first names, and last names of each of the two users in
    //            the top <num> pairs of users who are not friends but have a lot of
    //            common friends
    //        (B) For each pair identified in (A), find the IDs, first names, and last names
    //            of all the two users' common friends
    public FakebookArrayList<UsersPair> suggestFriends(int num) throws SQLException {
        FakebookArrayList<UsersPair> results = new FakebookArrayList<UsersPair>("\n");
        
        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll, FakebookOracleConstants.ReadOnly)) {
            /*
                EXAMPLE DATA STRUCTURE USAGE
                ============================================
                UserInfo u1 = new UserInfo(16, "The", "Hacker");
                UserInfo u2 = new UserInfo(80, "Dr.", "Marbles");
                UserInfo u3 = new UserInfo(192, "Digit", "Le Boid");
                UsersPair up = new UsersPair(u1, u2);
                up.addSharedFriend(u3);
                results.add(up);
            */
            stmt.executeUpdate(
                    "create view aug_friend as " +
                            "select f.user1_id, f.user2_id from "+ FriendsTable +" f " +
                            "        union " +
                            "select f.user2_id, f.user1_id from "+ FriendsTable +" f "
            );
            stmt.executeUpdate(
                    "create view temp as\n" +
                            "select * from (\n" +
                            "select count(*) as freq, user1, user2 from\n" +
                            "(select f1.user2_id as user1, f2.user2_id as user2, f1.user1_id as mutual\n" +
                            "from aug_friend f1, aug_friend f2\n" +
                            "where f1.user2_id<f2.user2_id and f1.user1_id=f2.user1_id and (f1.user2_id, f2.user2_id) not in\n" +
                            "(select f.user1_id, f.user2_id from " + FriendsTable + " f)) " +
                            "group by user1, user2\n" +
                            "order by freq desc, user1, user2)\n" +
                            "where ROWNUM <=" + num
            );
            ResultSet rst = stmt.executeQuery(
                    "select u1.user_id, u1.first_name, u1.last_name, u2.user_id, u2.first_name, u2.last_name " +
                            "from " + UsersTable + " u1, " + UsersTable + " u2, temp " +
                            "where u1.user_id=temp.user1 and u2.user_id=temp.user2 " +
                            "order by temp.freq desc, temp.user1, temp.user2"
            );

            Statement stmt2 = oracle.createStatement(FakebookOracleConstants.AllScroll, FakebookOracleConstants.ReadOnly);
            ResultSet rst2 = null;
            long u1_id, u2_id;
            while(rst.next()){
                u1_id = rst.getLong(1);
                u2_id = rst.getLong(4);
                UserInfo u1 = new UserInfo(u1_id,rst.getString(2),rst.getString(3));
                UserInfo u2 = new UserInfo(u2_id,rst.getString(5),rst.getString(6));
                UsersPair up = new UsersPair(u1, u2);

                rst2 = stmt2.executeQuery(
                        "select u.user_id, u.first_name, u.last_name from " + UsersTable + " u, " +
                         "(" +
                          "select a.user1_id as mutual_id from aug_friend a where a.user2_id= " + u1_id +
                                " intersect " +
                          "select a.user1_id as mutual_id from aug_friend a where a.user2_id= " + u2_id +
                         ") temp " +
                         "where u.user_id=temp.mutual_id " +
                         "order by u.user_id"
                );
                while(rst2.next()){
                    UserInfo u3 = new UserInfo(rst2.getLong(1),rst2.getString(2),rst2.getString(3));
                    up.addSharedFriend(u3);
                }
                results.add(up);
            }
            stmt.executeUpdate("drop view temp");
            stmt.executeUpdate("drop view aug_friend");

            rst.close();
            rst2.close();
            stmt2.close();
        }
        catch (SQLException e) {
            System.err.println(e.getMessage());
        }
        
        return results;
    }
    
    @Override
    // Query 7
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find the name of the state or states in which the most events are held
    //        (B) Find the number of events held in the states identified in (A)
    public EventStateInfo findEventStates() throws SQLException {
        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll, FakebookOracleConstants.ReadOnly)) {
            /*
                EXAMPLE DATA STRUCTURE USAGE
                ============================================
                EventStateInfo info = new EventStateInfo(50);
                info.addState("Kentucky");
                info.addState("Hawaii");
                info.addState("New Hampshire");
                return info;
            */
            ResultSet rst = stmt.executeQuery(
                    "select count(*) as freq, c.state_name " +
                       "from " + EventsTable + " e, " + CitiesTable + " c " +
                       "where e.event_city_id=c.city_id " +
                       "group by c.state_name " +
                       "order by freq desc, c.state_name asc"
            );

            rst.next();
            int most_num = rst.getInt(1);
            int temp_num;
            EventStateInfo info = new EventStateInfo(most_num);
            info.addState(rst.getString(2));

            while (rst.next()){
                temp_num = rst.getInt(1);
                if (temp_num==most_num){
                    info.addState(rst.getString(2));
                }else{
                    break;
                }
            }
            rst.close();
            return info;
        }
        catch (SQLException e) {
            System.err.println(e.getMessage());
            return new EventStateInfo(-1);
        }
    }
    
    @Override
    // Query 8
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find the ID, first name, and last name of the oldest friend of the user
    //            with User ID <userID>
    //        (B) Find the ID, first name, and last name of the youngest friend of the user
    //            with User ID <userID>
    public AgeInfo findAgeInfo(long userID) throws SQLException {
        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll, FakebookOracleConstants.ReadOnly)) {
            /*
                EXAMPLE DATA STRUCTURE USAGE
                ============================================
                UserInfo old = new UserInfo(12000000, "Galileo", "Galilei");
                UserInfo young = new UserInfo(80000000, "Neil", "deGrasse Tyson");
                return new AgeInfo(old, young);
            */
            ResultSet rst = stmt.executeQuery(
                    "select young.user_id, young.first_name, young.last_name " +
                    "from ( " +
                        "select u.user_id, u.first_name, u.last_name " +
                        "from " + FriendsTable +" f, " + UsersTable + " u " +
                        "where (f.user1_id=" + userID + " and u.user_id=f.user2_id) or (f.user2_id=" + userID + " and u.user_id=f.user1_id) " +
                        "order by u.year_of_birth desc, u.month_of_birth desc, u.day_of_birth desc, u.user_id desc ) young " +
                    "where rownum=1"
            );
            long id = 0;
            String fname = "";
            String lname = "";
            while(rst.next()){
                id = rst.getLong(1);
                fname = rst.getString(2);
                lname = rst.getString(3);
            }
            UserInfo young_friend = new UserInfo(id, fname, lname);
            rst = stmt.executeQuery(
                     "select old.user_id, old.first_name, old.last_name " +
                        "from ( " +
                            "select u.user_id, u.first_name, u.last_name " +
                            "from " + FriendsTable +" f, " + UsersTable + " u " +
                            "where (f.user1_id=" + userID + " and u.user_id=f.user2_id) or (f.user2_id=" + userID + " and u.user_id=f.user1_id) " +
                            "order by u.year_of_birth asc, u.month_of_birth asc, u.day_of_birth asc, u.user_id desc ) old " +
                        "where rownum=1"
            );

            while(rst.next()){
                id = rst.getLong(1);
                fname = rst.getString(2);
                lname = rst.getString(3);
            }
            UserInfo old_friend = new UserInfo(id, fname, lname);
            rst.close();
            return new AgeInfo(old_friend, young_friend);
        }
        catch (SQLException e) {
            System.err.println(e.getMessage());
            return new AgeInfo(new UserInfo(-1, "ERROR", "ERROR"), new UserInfo(-1, "ERROR", "ERROR"));
        }
    }
    
    @Override
    // Query 9
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find all pairs of users that meet each of the following criteria
    //              (i) same last name
    //              (ii) same hometown
    //              (iii) are friends
    //              (iv) less than 10 birth years apart
    public FakebookArrayList<SiblingInfo> findPotentialSiblings() throws SQLException {
        FakebookArrayList<SiblingInfo> results = new FakebookArrayList<SiblingInfo>("\n");
        
        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll, FakebookOracleConstants.ReadOnly)) {
            /*
                EXAMPLE DATA STRUCTURE USAGE
                ============================================
                UserInfo u1 = new UserInfo(81023, "Kim", "Kardashian");
                UserInfo u2 = new UserInfo(17231, "Kourtney", "Kardashian");
                SiblingInfo si = new SiblingInfo(u1, u2);
                results.add(si);
            */
            ResultSet rst = stmt.executeQuery(
                "select u1.user_id, u1.first_name, u1.last_name, " +
                        "u2.user_id, u2.first_name, u2.last_name " +
                "from " + UsersTable + " u1, " + UsersTable + " u2, " + FriendsTable + " f, " +
                        HometownCitiesTable + " h1, " + HometownCitiesTable + " h2 " +
                "where u1.last_name=u2.last_name and u1.user_id<u2.user_id " + //check last_name
                        "and u1.user_id=h1.user_id and u2.user_id=h2.user_id and h1.hometown_city_id=h2.hometown_city_id " + //check hometown
                        "and (u1.year_of_birth-u2.year_of_birth)<10 and (u2.year_of_birth-u1.year_of_birth)<10 " + //check birth year
                        "and u1.user_id=f.user1_id and u2.user_id=f.user2_id " +//check friendship
                        "order by u1.user_id asc, u2.user_id asc"
            );

            while(rst.next()){
                UserInfo user1 = new UserInfo(rst.getLong(1), rst.getString(2), rst.getString(3));
                UserInfo user2 = new UserInfo(rst.getLong(4), rst.getString(5), rst.getString(6));
                SiblingInfo si = new SiblingInfo(user1, user2);
                results.add(si);
            }
            rst.close();
        }
        catch (SQLException e) {
            System.err.println(e.getMessage());
        }
        
        return results;
    }
    
    // Member Variables
    private Connection oracle;
    private final String UsersTable = FakebookOracleConstants.UsersTable;
    private final String CitiesTable = FakebookOracleConstants.CitiesTable;
    private final String FriendsTable = FakebookOracleConstants.FriendsTable;
    private final String CurrentCitiesTable = FakebookOracleConstants.CurrentCitiesTable;
    private final String HometownCitiesTable = FakebookOracleConstants.HometownCitiesTable;
    private final String ProgramsTable = FakebookOracleConstants.ProgramsTable;
    private final String EducationTable = FakebookOracleConstants.EducationTable;
    private final String EventsTable = FakebookOracleConstants.EventsTable;
    private final String AlbumsTable = FakebookOracleConstants.AlbumsTable;
    private final String PhotosTable = FakebookOracleConstants.PhotosTable;
    private final String TagsTable = FakebookOracleConstants.TagsTable;
}
