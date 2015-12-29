/* @flow */

/*
    hnlinks - create database of URLs linked to on HN,
    Copyright (C) 2015-2016 Matt Howlett

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

"use strict"

var levelup = require('levelup');
var fs = require('fs');
var KafkaRest = require("kafka-rest");

var db_links = levelup('/data/hnlinks_links');
var kafka = new KafkaRest({ 'url': 'http://localhost:8082' });

kafka.consumer("mygroup").join(
  
  {
    "format": "avro",
    "auto.offset.reset": "smallest",
    "auto.commit.enable": "true"
  }, 
  
  function(err, consumer_instance) {
        
    var stream = consumer_instance.subscribe('hnitems')
    stream.on('data', function(msgs) {
      for (var i = 0; i<msgs.length; i++) {
        console.log("Got a message: key=" + msgs[i].key + ":");
        console.log(" " + msgs[i].value.type);
        console.log(" " + msgs[i].value.urls);
        console.log(" " + msgs[i].value.score);
        console.log(" " + msgs[i].value.children);
      }
    });
    stream.on('error', function(err) {
      console.log("Something broke: " + err);
    });
    stream.on('end', function() {
      console.log("Consumer stream closed.");
    });
    
    consumer_instance.on('end', function() {
      console.log("Consumer instance closed.");
    });
    
    process.on('SIGINT', function() {
      console.log("Attempting to shut down consumer instance...");
      consumer_instance.shutdown(logShutdown);
    });
  }
  
);

function logShutdown(err) {
  if (err) {
    console.log("Error while shutting down: " + err);
  }
  else {
    console.log("Shutdown cleanly.");
  }
}


  /*
  db.get(info.url, function(err, value) {
    if (err) {
      // assume doesn't exist, so just add.
      //console.log("don't know about, adding:");
      //console.log(info.urlType + " " + info.id + " " + info.score + " " + info.url);
      db.put(info.url, info.urlType + " " + info.id + " " + info.score, function() {});
    }
    else {
      let existingInfo = parseInfo(value);
      existingInfo.url = info.url;

      if (compareOccurances(info, existingInfo) <= 0) {
        //console.log("new info not better than existing:");
        //console.log(info.urlType + " " + info.id + " " + info.score + " " + info.url);
      }
      else {
        //console.log("GOT BETTER INFO!: ");
        //console.log(info.urlType + " " + info.id + " " + info.score + " " + info.url);
        db.put(info.url, info.urlType + " " + info.id + " " + info.score, function() {});
      }
    }
  });  
  */
  
  
  /*
  
function parseInfo(infoStr) {
  let bits = infoStr.split(' ');
  return { urlType: bits[0], id: parseInt(bits[1]), score: parseInt(bits[2]) };
}

function handleNewUrl(info) {
  db.get(info.url, function(err, value) {
    if (err) {
      // assume doesn't exist, so just add.
      //console.log("don't know about, adding:");
      //console.log(info.urlType + " " + info.id + " " + info.score + " " + info.url);
      db.put(info.url, info.urlType + " " + info.id + " " + info.score, function() {});
    }
    else {
      let existingInfo = parseInfo(value);
      existingInfo.url = info.url;

      if (compareOccurances(info, existingInfo) <= 0) {
        //console.log("new info not better than existing:");
        //console.log(info.urlType + " " + info.id + " " + info.score + " " + info.url);
      }
      else {
        //console.log("GOT BETTER INFO!: ");
        //console.log(info.urlType + " " + info.id + " " + info.score + " " + info.url);
        db.put(info.url, info.urlType + " " + info.id + " " + info.score, function() {});
      }
    }
  });  
}

function compareOccurances(a, b) {
  let aScore = a.score;
  let bScore = b.score;
  if (a.mentionType == 'S') { aScore *= 8; }
  if (b.mentionType == 'I') { bScore *= 8; }
  console.log("a vs b: " + aScore + " " + bScore)
	
  return a.score == b.score 
    ? 0
    : (a.score < b.score 
      ? -1
      : 1);
}

*/