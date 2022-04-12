use slserver;
db.pm.pipeline_rt.createIndex({"region": 1, "_id": "hashed"});
sh.enableSharding("slserver");
sh.shardCollection("slserver.pm.pipeline_rt", {"region": 1, "_id": "hashed"});
sh.addShardTag("us-west-2", "oregon");
sh.addShardTag("ap-southeast-1", "singapore");
sh.addTagRange("slserver.pm.pipeline_rt", {"region": "oregon", "_id": MinKey}, {"region": "oregon", "_id": MaxKey}, "oregon");
sh.addTagRange("slserver.pm.pipeline_rt", {"region": "singapore", "_id": MinKey}, {"region": "singapore", "_id": MaxKey}, "singapore");