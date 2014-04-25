var taras_s3 = require("taras-s3");
const AWS = require('aws-sdk');
var fs = require("fs");
var async = require("async");

var config = JSON.parse(fs.readFileSync(process.argv[2]));
var s3 = new AWS.S3(config.aws);
//console.log(s3.config)

var stats = {}

function expireBucket(bucket, callback) {
  function expire(ls, callback) {
    var now = Date.now() / 1000;
    var dead = ls.filter(function (x) {
      return (new Date(x.LastModified).getTime()/1000 + config.expireAge) < now;
    });
    dead = dead.map(function (x) {return {'Key':x.Key}});

    var deadlsls = taras_s3.chunkArray(dead, 1000);

    console.log(deadlsls.length, dead.length, ls.length);

    stats.keysToDelete = dead.length;
    stats.deleteStart = Date.now();

    function delete1k(deadls, callback) {
      s3.deleteObjects({'Bucket':bucket,
                        'Delete':{ 'Objects': deadls}},
                       callback);
    }
    async.map(deadlsls, delete1k, callback);
  }
  taras_s3.S3ListObjects(s3, {'Bucket':bucket},
                         function (err, ls) {
                           if (err)
                             throw err;
                           expire(ls, callback);
                         });
  
}

function printSummary() {
  if (stats.uploadStart) {
    var ms = stats.uploadFinish - stats.uploadStart
    var msPerUpload = ms/stats.keysToUpload;
    console.log(ms + "ms to upload " + stats.keysToUpload + " "+msPerUpload+"ms per upload");
  }

  if (stats.deleteStart) {
    var ms = stats.deleteFinish - stats.deleteStart
    var msPerDelete = ms/stats.keysToDelete;
    console.log(ms + "ms to delete " + stats.keysToDelete + " "+msPerDelete+"ms per delete");
  }
}

function expire() {
  async.map(config.expireBuckets, expireBucket, function (err, ret) {
              if (err)
                throw err;

              var merged = [];
              merged = merged.concat.apply(merged, ret);
              ret = merged.map(function(x) {return x.Deleted});
              merged = []
              merged = merged.concat.apply(merged, ret);
              console.log("Deleted " + merged.length + " objects")
              stats.deleteFinish = Date.now();
              printSummary();
            });
}


function upload(length) {
  var arr = [];
  stats.keysToUpload = length;

  for (var i = 0;i<length;i++)
    arr[i]= {Key: 'node'+i, Body: 'Hello '+i, Bucket:config.expireBuckets[0]};
  stats.uploadStart = Date.now();
  async.map(arr,
            function(obj, callback) {s3.putObject(obj, callback)},
            function(err){
              if (err) throw err;
              console.log("uploaded "+length+ " objects")
              stats.uploadFinish = Date.now();
              expire();
            });
}

if(process.argv.length == 4) {
  //benchmark
  upload(200);
} else {
  expire();
}

