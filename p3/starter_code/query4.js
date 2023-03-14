
// query 4: find user pairs (A,B) that meet the following constraints:
// i) user A is male and user B is female
// ii) their Year_Of_Birth difference is less than year_diff
// iii) user A and B are not friends
// iv) user A and B are from the same hometown city
// The following is the schema for output pairs:
// [
//      [user_id1, user_id2],
//      [user_id1, user_id3],
//      [user_id4, user_id2],
//      ...
//  ]
// "user_id" is the field from the users collection that you should use. 
// Do not use the "_id" field in the users collection.
  
function suggest_friends(year_diff, dbname) {
    db = db.getSiblingDB(dbname);
    var pairs = [];
    // Return an array of arrays.
    const cursor1 = db.users.find({gender: "male"},{user_id:1, YOB:1, friends:1, hometown:1, _id:0});
    cursor1.forEach((user1)=>{
        const cursor2 = db.users.find({
            gender: "female",
            $and: [{YOB: {$lt: user1.YOB + year_diff}}, {YOB: {$gt: user1.YOB - year_diff}}],
            hometown: user1.hometown
        },{friends:1, user_id:1, _id:0});
        cursor2.forEach((user2)=>{
            //check for non-friend constraints
            if(user2.friends.indexOf(user1.user_id)===-1 && user1.friends.indexOf(user2.user_id)===-1){
                pairs.push([user1.user_id, user2.user_id]);
            }
        });
    });
    return pairs;
}
