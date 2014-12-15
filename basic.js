var async = require('async'),
	mongoose = require('mongoose');

var raw_schema = new mongoose.Schema({ user: String, item: String, value: Number });
var target_mongodb = 'localhost/test';
var raw_mongodb_collection = 'raw';
var aggregate_mongodb_collection = 'out';
var record_array = [
/*
	{ user: "a", item: "1", value: 0.3},
	{ user: "a", item: "3", value: 0.5},
	{ user: "a", item: "4", value: 0.9},
	{ user: "b", item: "2", value: 0.1},
	{ user: "b", item: "3", value: 0.6},
	{ user: "b", item: "5", value: 0.2},
	{ user: "c", item: "3", value: 0.2},
	{ user: "c", item: "4", value: 0.7},
*/
	{ user: "a", item: "1", value: 1},
	{ user: "a", item: "1", value: 1},
	{ user: "a", item: "1", value: 1},

	{ user: "a", item: "3", value: 1},
	{ user: "a", item: "3", value: 1},
	{ user: "a", item: "3", value: 1},
	{ user: "a", item: "3", value: 1},
	{ user: "a", item: "3", value: 1},

	{ user: "a", item: "4", value: 1},
	{ user: "a", item: "4", value: 1},
	{ user: "a", item: "4", value: 1},
	{ user: "a", item: "4", value: 1},
	{ user: "a", item: "4", value: 1},
	{ user: "a", item: "4", value: 1},
	{ user: "a", item: "4", value: 1},
	{ user: "a", item: "4", value: 1},
	{ user: "a", item: "4", value: 1},

	{ user: "b", item: "2", value: 1},

	{ user: "b", item: "3", value: 1},
	{ user: "b", item: "3", value: 1},
	{ user: "b", item: "3", value: 1},
	{ user: "b", item: "3", value: 1},
	{ user: "b", item: "3", value: 1},
	{ user: "b", item: "3", value: 1},

	{ user: "b", item: "5", value: 1},
	{ user: "b", item: "5", value: 1},

	{ user: "c", item: "3", value: 1},
	{ user: "c", item: "3", value: 1},

	{ user: "c", item: "4", value: 1},
	{ user: "c", item: "4", value: 1},
	{ user: "c", item: "4", value: 1},
	{ user: "c", item: "4", value: 1},
	{ user: "c", item: "4", value: 1},
	{ user: "c", item: "4", value: 1},
	{ user: "c", item: "4", value: 1},
];

async.series([
	// reset & insert
	function(callback) {
		var conn = mongoose.createConnection('mongodb://'+target_mongodb);
		conn.on('error', console.error.bind(console, 'connection error:'));
		conn.once('open', function() {
			var model = conn.model( 'rec', raw_schema, raw_mongodb_collection);
			model.remove({}, function(){
				model.collection.insert(record_array, {w:1}, function (err) {
					if (err)
						console.log(err);
					conn.close();
					if (callback)
						callback(0, 'finish insert: '+record_array.length);
				});
			});
		});
	},
	function(callback) {
		var conn = mongoose.createConnection('mongodb://'+target_mongodb);
		conn.on('error', console.error.bind(console, 'connection error:'));
		conn.once('open', function () {
			var model = conn.model( 'rec', raw_schema, raw_mongodb_collection);
			var aggregate = model.aggregate(
				[
					{ 
						$group: {
							_id: {
								user: "$user",
								item: "$item"
							}, 
							value: {
								$sum: 
								{
									$multiply: [
										"$value"
										,
										0.1
									]
								} 
							}
						}
					},
					{
						$sort: { _id: 1}
					}
				]
			);
			//aggregate.allowDiskUse(true).exec( function(err, data){
			//	console.log(data);
			//	if (err)
			//		console.log(err);
			//	conn.close();
			//	callback(0, 'aggregate done');
			//});
			var cursor = aggregate.allowDiskUse(true).cursor({ batchSize: 4 }).exec();
			cursor.on('data', function(data) {
				console.log("data", data);
			}).on('end', function(err) {
				if (err)
					console.log("end", err);
				conn.close();
				callback(0, 'aggregate done');
			});
		});
	},
/*
	function(callback) {
		var conn = mongoose.createConnection('mongodb://'+target_mongodb);
		conn.on('error', console.error.bind(console, 'connection error:'));
		conn.once('open', function () {
			var model = conn.model( 'rec', raw_schema, raw_mongodb_collection);
			var aggregate = model.aggregate(
				[
					{ 
						$group: {
							_id: {
								user: "$user",
								item: "$item"
							}, 
							value: {
								$sum: 
								{
									$multiply: [
										"$value"
										,
										0.1
									]
								} 
							}
						}
					},
					{
						$sort: { _id: 1}
					},
					{
						$out: aggregate_mongodb_collection
					}
				]
			);
			aggregate.allowDiskUse(true).exec( function(err, data){
				if (err)
					console.log(err);
				conn.close();
				callback(0, 'aggregate done');
			});
		});
	},
// */
], function(err, result){
	if (err)
		console.log(err);
	for (var i=0, cnt=result.length ; i<cnt ; ++i)
		console.log("Part"+i+":", result[i]);
});
