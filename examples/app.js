// ---------------------------
// Riakpb test program
// Copyright by Javier Arevalo in 2012.
// - http://www.iguanademos.com/Jare/
// - @TheJare on twitter
// Licensed under the MIT license: http://www.opensource.org/licenses/mit-license.php
// ---------------------------

var Riak = require('../');
var Client = Riak.Client;

var client = new Riak.Client('localhost', 8087);

client.GetServerInfo(function(err, res) { console.log("GetServerInfo response: ", res); });
client.GetClientId(function(err, res) { console.log("GetClientId response: ", res); });
client.SetClientId("Jarete", function(err, res) { console.log("SetClientId response: ", res); });
client.GetClientId(function(err, res) { console.log("GetClientId response: ", res); });
client.ListBuckets(function(err, res) { console.log("ListBuckets response: ", res); });
client.GetBucket("users", function(err, res) { console.log("GetBucket users response: ", res); });
client.SetBucket("users", {allow_mult:false, n_val:4}, function(err, res) { console.log("SetBucket users response: ", res); });
client.GetBucket("users", function(err, res) { console.log("GetBucket users response: ", res); });
client.ListKeys("users", function(err, res) { console.log("users response: ", res); });
client.ListKeys("flights", function(err, res) { console.log("flights response: ", res); });
client.Put("flights", "KLM-5034", "Departing soon", {}, function(err, res) { console.log("put KLM-5034 response: ", res); });
client.Get("flights", "KLM-5034", {}, function(err, res) { console.log("get KLM response: ", res); });
client.Get("users", "jarelol", {}, function(err, res) { console.log("get jarelol response: ", res); });
client.Put("users", "nodetestuser", "Hi! Itsa me Mario!", {}, function(err, res) { console.log("put nodetestuser response: ", res); });
client.Get("users", "nodetestuser", {}, function(err, res) { console.log("get nodetestuser response: ", res); });
client.PutNokey("users", "Autogen Key", {return_body:true}, function(err, res) { console.log("put Autogen key response: ", res); });
client.Put("users", "2iuser_1", "Secondary Indexes 1", {content:{indexes:["myindex_int", 1]}}, function(err, res) { console.log(err, "put 2iuser_1 response: ", res); });
client.PutIndex("users", "2iuser_20", "Secondary Indexes 20", [["myindex_int", 20]], {}, function(err, res) { console.log("put 2iuser_20 response: ", res); });
client.PutIndex("users", "2iuser_35", "Secondary Indexes 35", [["myindex_int", 35], ["myotherindex_bin", "2ivalue"]], {}, function(err, res) { console.log("put 2iuser_35 response: ", res); });
client.Get("users", "2iuser_35", {}, function(err, res) { console.log("get 2iuser_35 response: ", res, "indexes: ", res.content[0].indexes); });
client.IndexQuery("users", "myindex_int", "001", function(err, res) { console.log("indexQuery response: ", res); });
client.IndexQueryRange("users", "myindex_int", 15, 30, function(err, res) { console.log("indexQueryRange 15-30 response: ", res); });
client.IndexQueryRange("users", "myindex_int", 20, 37, function(err, res) { console.log("indexQueryRange 20-37 response: ", res); });

client.MapReduce(
'{"inputs":[["users","jarelol"],["users","jare"],["users","p5"]]' +
',"query":[{"map":{"language":"javascript","source":"' +
'function(value) {' +
'  return [value.values[0].data];' +
'}' +
'"}},{"reduce":{"language":"javascript","source":"' +
'function(values) {' +
'  var r = \'\';' +
'  for (i in values) r += values[i];' +
'  return [r];' +
'}' +
'"}}]}', 'application/json', function(err, res) { console.log("MapReduce response: ", res); });

client.MapReduce(
'{"inputs":"users"' +
',"query":[{"map":{"language":"javascript","source":"' +
'function(v) {' +
'  var m = v.values[0].data.toLowerCase().match(/\\w*/g);' +
'  var r = [];' +
'  for(var i in m) {' +
'    if(m[i] != \'\') {' +
'      var o = {};' +
'      o[m[i]] = 1;' +
'      r.push(o);' +
'    }' +
'  };' +
'  return r;' +
'}' +
'"}},{"reduce":{"language":"javascript","source":"' +
'function(v) {' +
'  var r = {};' +
'  for(var i in v) {' +
'    for(var w in v[i]) {' +
'      if(w in r) r[w] += v[i][w]; ' +
'      else r[w] = v[i][w];' +
'    }' +
'  };' +
'  return [r];' +
'}' +
'"}}]}', 'application/json', function(err, res) { console.log("MapReduce response: ", res); });

client.Ping(function(err, res) { console.log("PONG"); client.end(); });
