// query6 : Find the average user friend count
//
// Return a decimal as the average user friend count of all users
// in the users document.

function find_average_friendcount(dbname){
  db = db.getSiblingDB(dbname);
  // TODO: return a decimal number of average friend count
  var count = 0.0;
  var sumFriends = 0.0;
  const cursor = db.users.find({}, {"friends": 1, _id:0});
  cursor.forEach((userInfo)=>{
    const jsonFri = JSON.stringify(userInfo);
    const parsedFri = JSON.parse(jsonFri);
    count++;
    sumFriends += parsedFri.friends.length;
  });
  return sumFriends/count;
}
