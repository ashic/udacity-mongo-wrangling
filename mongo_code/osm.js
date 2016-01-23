conn = new Mongo();
db = conn.getDB("osm");

var cuisine_entries = db.raw1.find({'cuisine': {$exists:1}}).length();
print(cuisine_entries);

var cuisines = db.raw1.aggregate([
		{$match: {'cuisine': {$exists:1}}},
		{$group: { _id: "$cuisine", 'count': {$sum:1} } }
	]);


cuisines.forEach(printjson);

var cleanup_cuisine_name = function(c){
	var lowered = c.toLowerCase();
	var split = lowered.split(/[,;&]+/)
	for(var s in split){
		split[s] = split[s].trim()
		if(split[s] && split[s][0] == '_') split[s] = split[s].substring(1);
		if(split[s] && split[s][split[s].length-1] == '_') 
			split[s] = split[s].substring(0, split[s].length-1);
		if(split[s] == "british_food") split[s] = "british";
	}

	return split;
}

db.createCollection("cuisines");
db.cuisines.createIndex({cuisine:1});

db.raw1.find({'cuisine': {$exists:1}}).snapshot().forEach(function(el){
 	el.cuisine = cleanup_cuisine_name(el.cuisine);
 	db.cuisines.save(el);
});


//Data cleansing 2 - postcodes.
db.raw1.aggregate([{
		$match: {
		"address.postcode" : {
			$regex:/(SE2|SE3|SE7|SE8|SE9|SE10|SE12|SE13|SE18|SE28|DA15|DA16|BR7)\s.*/ 
			}
		}
	},
	{$out:"greenwich"}
]);


var popular_cuisines = db.cuisines.aggregate([
		{$match: {'cuisine': {$exists:1}}},					//only entries with cuisines
		{$group: { _id: "$cuisine", 'count': {$sum:1} } },	//one for each array
		{$unwind: "$_id"},									//arrays unwinded
		{$group: {_id:"$_id", count: {$sum: "$count"}}},	// grouped count after unwinding
		{$sort: {count: -1}},								
		{$out: "popular_cuisines"}							//store to result collection for easy querying
	]);

popular_cuisines.forEach(printjson);
