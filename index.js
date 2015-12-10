/*
    hnlink - create database of URLs linked to on HN,
    Copyright (C) 2015 Matt Howlett

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
var Firebase = require("firebase");
var fs = require('fs');
var db = levelup('/data/hnlink');

const maxOutstandingRequests = 100;

if (process.argv[process.argv.length-1] == "info") {
	displayInfo();
}
else {
	fetch("https://hacker-news.firebaseio.com/v0/maxitem.json")
	  .then(function(res) { return res.json(); })
      .then(function(json) {
		 crawl(parseInt(json)); 
	  })
	  .catch(function(err) {
		  console.log(err);
	  })
}

function displayInfo() {
  let cnt = 0;
  db.createReadStream()
    .on('data', function (data) {
	  cnt += 1;
      console.log(data.key, '=', data.value)
    })
    .on('error', function (err) {
      console.log('Oh my!', err)
    })
    .on('close', function () {
      console.log('Stream closed');
	  console.log("count: " + cnt);
    })
    .on('end', function () {
      console.log('Stream end');
    });
}

function crawl(maxId) {
  db.get('__last_index_fetched__', function(err, value) {
    let index = 0;
    if (err) {
	  db.put('__last_index_fetched__', 0, function(err) {
		  console.log("error setting __last_index_fetched__, starting from item #1", err);
	  });
    }
    else {
	  index = parseInt(value) - maxOutstandingRequests;
	  if (index < 0) { index = 0; }
	  console.log("starting from item #" + index);
    }
    mainLoop(index, maxId);
  });
}

function mainLoop(index, maxIndex) {
  let numOpen = 0;
  
  setInterval(function tick() {
  	while (numOpen < maxOutstandingRequests) {
      numOpen += 1;
	  index += 1;
	  if (index > maxIndex) {
		  console.log("finished at: " + maxIndex);
		  process.exit();
	  }
   	  db.put('__last_index_fetched__', index, function() {});
	  
	  fetch("https://hacker-news.firebaseio.com/v0/item/" + index + ".json", {timeout: 5000})
        .then(function(res) { return res.json(); })
        .then(function(json) {
  		  numOpen -=1;
	      // fs.appendFile("./tmp.txt", json.text + " \n\n");

	      // we have a story!
	      if (json.url && json.id) {
		    handleNewUrl({url: json.url, id: json.id, score: json.score, urlType: 'S'});
	      }
	  
	      // check text for links. 
	      if (json.text) {
		    let kidsCount = 0;
		    if (json.kids) {
		      kidsCount = json.kids.length;
		    }
		    let loc = 0;
		    let idx = json.text.indexOf("href=\"", loc);
		    for (; idx != -1; idx = json.text.indexOf("href=\"", loc)) {
		      let idx2 = json.text.indexOf("\"", idx+6);
		      let url = json.text.substring(idx+6, idx2);
	  		  handleNewUrl({url: url, id: json.id, score: kidsCount, urlType: 'I'});
 		      loc = idx2 + 1;
		    }
		  }
	    })
	    .catch(function(err) {
	  	  numOpen -= 1;
		  console.log(err);
	    });
	}
  }, 1000);
  
};

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
