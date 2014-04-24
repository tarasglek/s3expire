var taras_s3 = require("taras-s3");
const AWS = require('aws-sdk');
var fs = require("fs");
var async = require("async");

var config = JSON.parse(fs.readFileSync(process.argv[2]));
var s3 = new AWS.S3(config.aws);

function expireBucket(bucket, callback) {
  function expire(ls, callback) {
    var now = Date.now() / 1000;
    var dead = ls.filter(function (x) {
      return (new Date(x.LastModified).getTime()/1000 + config.expireAge) < now;
    });
    dead = dead.map(function (x) {return {'Key':x.Key}});

    var deadlsls = taras_s3.chunkArray(dead, 1000);

    console.log(deadlsls.length, dead.length, ls.length);

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

async.map(config.expireBuckets, expireBucket, function (err, ret) {
  if (err)
    throw err;

  var merged = [];
  merged = merged.concat.apply(merged, ret);
  ret = merged.map(function(x) {return x.Deleted});
  merged = []
  merged = merged.concat.apply(merged, ret);
  console.log("Deleted " + merged.length + " objects")
  });

