// Find the oldest friend for each user who has a friend.
// For simplicity, use only year of birth to determine age. If there is a tie, use the friend with the smallest user_id
// Return a javascript object : the keys should be user_ids and the value for each user_id is their oldest friend's user_id
// You may find query 2 and query 3 helpful. You can create selections if you want. Do not modify the users collection.
//
//You should return something like this:(order does not matter)
//{user1:oldestFriend1, user2:oldestFriend2, user3:oldestFriend3,...}

function find_YOB(id){
    var BreakException = {};
    var yob = [];
    try {
        db.users.find().forEach( function (doc) {
            if(doc.user_id === id){
                yob.push(doc.YOB);
                throw BreakException;
            }
        });
    } catch (e) {
        if (e !== BreakException) throw e;
    }
    return yob[0];
}

function oldest_friend(dbname){
    db = db.getSiblingDB(dbname);
    var results = {};
    // unwind friends
    db.users.aggregate( [
        { $unwind: { path: "$friends" } },
        { $project : { _id : 0} },
        { $project : { user_id: 1, friends : 1 } },
        { $out : "flat_users" }
    ] );
    db.flat_users.find().forEach( function (doc) {
        if(!results.hasOwnProperty(doc.user_id)){
            results[doc.user_id] = doc.friends;
        } else{
            const cur_friend = results[doc.user_id];
            const new_YOB = find_YOB(doc.friends)
            const cur_YOB = find_YOB(cur_friend);
            if(new_YOB < cur_YOB){
                results[doc.user_id] = doc.friends;
            } else{
                if(new_YOB === cur_YOB){
                    if(doc.friends < cur_friend){
                        results[doc.user_id] = doc.friends;
                    }
                }
            }
        }
        if(!results.hasOwnProperty(doc.friends)){
            results[doc.friends] = doc.user_id;
        } else{
            const cur_friend1 = results[doc.friends];
            const new_YOB = find_YOB(doc.user_id);
            const cur_YOB = find_YOB(cur_friend1);
            if(new_YOB < cur_YOB){
                results[doc.friends] = doc.user_id;
            } else{
                if(new_YOB === cur_YOB){
                    if(doc.user_id < cur_friend1){
                        results[doc.friends] = doc.user_id;
                    }
                }
            }
        }
    });
    return results;
}