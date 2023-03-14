// query1 : find users whose hometown city is the specified city. 

function find_user(city, dbname){
    db = db.getSiblingDB(dbname);
    var results = [];
    db.users.find().forEach( function (doc) {
        if(doc.hometown.city === city){
            results.push(doc.user_id)
        }
    })
    // The result will be an array of integers. The order does not matter.
    return results;
}