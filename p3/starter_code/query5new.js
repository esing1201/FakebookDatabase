// Find the oldest friend for each user who has a friend. 
// For simplicity, use only year of birth to determine age. If there is a tie, use the friend with the smallest user_id
// Return a javascript object : the keys should be user_ids and the value for each user_id is their oldest friend's user_id
// You may find query 2 and query 3 helpful. You can create selections if you want. Do not modify the users collection.
//
//You should return something like this:(order does not matter)
//{user1:oldestFriend1, user2:oldestFriend2, user3:oldestFriend3,...}

function oldest_friend(dbname){
  db = db.getSiblingDB(dbname);
  var results = {};
  // return an javascript object described above
  db.users.aggregate( [
    { $unwind: { path: "$friends" } },
    { $project : { _id : 0} },
    { $project : { user_id: 1, friends : 1 } },
    { $out : "flat_users" }
  ] );
  db.flat_users.aggregate( [
    { $lookup: { from: "users",
                 localField: "user_id",
                 foreignField: "user_id",
                 as: "friend_info"} },
    { $group:{"_id":"$user_id",
        "min_num_sold":{$min:"$friend_info.YOB"},
        "records":{$push:"$$ROOT"}}},
    { $redact:{$cond:[{$eq:[{$ifNull:["$num_sold","$$ROOT.min_num_sold"]},
            "$$ROOT.min_num_sold"]},
          "$$DESCEND","$$PRUNE"]}},
  ] )

  return results;
}