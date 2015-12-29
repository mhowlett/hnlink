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
var fetch = require('node-fetch');
var fs = require('fs');
var KafkaRest = require("kafka-rest");

var db_inprogress = levelup('/data/hnlinks_inprogress');
var db_errors = levelup('/data/hnlinks_errors');
var kafka = new KafkaRest({ 'url': 'http://localhost:8082' });

const maxOutstandingRequests = 100;
var totalErrorCount = 0;

var hnItemKeySchema = new KafkaRest.AvroSchema({"name": "hn_item_key", "type": "int"});
var hnItemValueSchema = new KafkaRest.AvroSchema({
    "doc": "selected information extracted from a hacker news item.", 
    "name": "hn_item_value",
    "type": "record",
    "fields": [
        { "doc": "What type of item this is.",
          "name": "type", "type": {"type": "enum", "name": "hn_item_type", "symbols": ["COMMENT", "STORY", "OTHER"] } },
          
        { "doc": "All URLs referenced by the item.",
          "name": "urls", "type": {"type": "array", "items": "string"} },
          
        { "doc": "If type == STORY then number of up-votes, else undefined.",
          "name": "score", "type": "int" },
          
        { "doc": "ids of children of this item",
          "name": "children", "type": {"type": "array", "items": "int"} },
    ]
});

fetch("https://hacker-news.firebaseio.com/v0/maxitem.json")
  .then(function(res) { return res.json(); })
  .then(function(json) {
    readFromInProgress(parseInt(json)); 
  })
  .catch(function(err) {
    console.log(err);
  });
    
function readFromInProgress(maxId) {
  var inprogress = [];  
  var lastFetchedIndex = 0;
  
  db_inprogress.createReadStream()
    .on('data', function (data) {
      if (!data.key.startsWith("__")) {
        inprogress.push(parseInt(data.key));
      } else if (data.key == "__last_fetched_index__") {
        lastFetchedIndex = parseInt(data.value);
      }
    })
    .on('error', function (err) {
      console.log('ASSERT: error not expected reading inprogress stream', err)
      process.exit();
    })
    .on('close', function () {
      mainLoop(lastFetchedIndex, inprogress, maxId);
    })
    .on('end', function () {
      // nothing to do.
    });
}

function mainLoop(index, inprogress, maxIndex) {
  let numOpen = 0;
  
  setInterval(function tick() {
    while (numOpen < maxOutstandingRequests) {
      var toGet;
      if (inprogress.length == 0) {
        index += 1;
        toGet = index;
        if (index > maxIndex && numOpen == 0) {
          console.log("finished at: " + maxIndex);
          process.exit();
        }
        else if (index > maxIndex) {
          break;
        }
        db_inprogress.put("__last_fetched_index__", index, function(err) { if (err) { console.log("ASSERT: unexpected error setting __last_fetched_index__", err); process.exit(); } });
        db_inprogress.put(index.toString(), 0, function(err) { if (err) { console.log("ASSERT: unexpected error setting in inprogress: " + index, err); process.exit(); } });
      }
      else {
        toGet = inprogress[inprogress.length - 1];
        inprogress.splice(inprogress.length-1, 1);
        console.log("getting prev in progress: " + toGet);
      }
      numOpen += 1;   
      fetchId(toGet);
    }
  }, 1000);

  function fetchId(id) {       
    fetch("https://hacker-news.firebaseio.com/v0/item/" + id + ".json", {timeout: 5000})
      .then(function(res) { return res.json(); })
      .then(function(json) {
        numOpen -= 1;
        db_inprogress.del(json.id, function(err) { if (err) { console.log("ASSERT: error not expected deleting from inprogress: " + json.id, err); process.exit(); } });
          
        // for human consumption.
        fs.writeFile("/data/hnlinks_lastgot.txt", json.id + " \n\n");

        if (!json.kids) {
          json.kids = [];
        }
          
        if (json.url && json.id) {
          // we have a story.
          handleNewUrl({urls: [json.url], id: json.id, kids: json.kids, score: json.score, urlType: 'STORY'});
        }
  
        // check text for links. 
        if (json.text) {
          let kidsCount = 0;
          if (json.kids) {
            kidsCount = json.kids.length;
          }
          let urls = [];
          let loc = 0;
          let idx = json.text.indexOf("href=\"", loc);
          for (; idx != -1; idx = json.text.indexOf("href=\"", loc)) {
            let idx2 = json.text.indexOf("\"", idx+6);
            if (idx2 < idx + 6) { break; }
            let url = json.text.substring(idx+6, idx2);
            urls.push(url);
            loc = idx2 + 1;
          }
          handleNewUrl({urls: urls, id: json.id, kids: json.kids, score: kidsCount, urlType: 'COMMENT'});
        }
      })
      .catch(function(error) {
        numOpen -= 1;

        db_errors.get(id, function(err, value) {
          let eCnt = 0;
          if (!err) {
            eCnt = parseInt(value);
          }
          eCnt += 1;
          
          totalErrorCount += 1;
          if (totalErrorCount > 50) {
            console.log("too many errors, quitting");
            process.exit();
          }
          
          console.log("firebase fetch error: " + error);
          
          db_errors.put(id, eCnt, function(err) { if (err) { console.log("ASSERT: error not expected setting error count for: " + id, err); process.exit(); } });
        });    
      });  
  }
  
};


function handleNewUrl(info) {
  
  kafka.topic('hnitems').produce(hnItemKeySchema, hnItemValueSchema, 
    { 'key': info.id, 
      'value': 
       {
           'type': info.urlType,
           'urls': info.urls,
           'score': info.score,
           'children': info.kids
       }
    },
    function(e,r) {
      if (e) {
        console.log(e);
      }
    }
  );
  
}


