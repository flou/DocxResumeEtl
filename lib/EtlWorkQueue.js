var util = require('util');
var async = require('async');
var spawn = require('child_process').spawn;
var exec = require('child_process').exec;



var bind = function (fun, thisArg) {
	return  function () {
		return fun.apply(thisArg || null, arguments);
	};
};

var EtlWorkQueue = function(extractTransformBinary, concurrency, log) {
	var self = this;
	
	this._log = log;
	this._extractTransformBinary = extractTransformBinary;
	
	this._fileQueue = async.queue(bind(this._processFile, this), concurrency);
	this._fileQueue.drain = function() {
	    log.debug('All items have been processed, gimme more !');
	};
};

EtlWorkQueue.prototype = {
	
	addFile: function(file, cb) {
		this._fileQueue.push(file, cb || function() {});
	},
	
	_processFile: function(file, cb) {

		var self = this;
		var log = this._log;

		log.info('Processing file : ' + file);
		
		console.time('Process file : ' + file);
		async.waterfall([
			function(cb) {
				cb(null, file);
			},
			bind(this._extractTransform, this),
			bind(this._load, this)
		], function(err) {
			console.timeEnd('Process file : ' + file);
			if (err) {
				log.warning(util.inspect(err));
			} else {
				log.info('File : ' + file + ' has been processed successfully');
			}
			
			cb(err);
		});
		
	},
	
	_extractTransform: function(file, cb) {
		this._log.info('Starting extraction and transform...');
		
		var self = this;
		var stdout = '';
		var stderr = '';
		var extractor = spawn(this._extractTransformBinary, [file]);
		
		extractor.stdout.on('data', function(chunk) {
			stdout += chunk;
		});
		
		extractor.stderr.on('data', function(chunk) {
			stderr += chunk;
		});
		
		extractor.on('exit', function(code, signal) {
			self._log.debug(code + ' ' + signal);
			self._log.debug(stderr);
			if (!code || code === 0) {
				self._log.debug('Extracted : ' + stdout);
				cb(null, stdout);
			} else {
				cb('Process terminated with code : ' + code + ' and signal : ' + signal);
			}
		});
		
	},
	
	_load: function(doc, cb) {
		this._log.info('Starting load...');
    var Log = require('coloured-log');
    var config = require('../config');
    var log = new Log(config.logLevel);

    var Client = require('mysql').Client;
    var client = new Client(config.mysql);
    client.connect();
    
    sys = require('sys');
    var json_cv = JSON.parse(JSON.parse(doc));    
    var collaborateur_id = 0;

    async.waterfall([
      // function(callback) {
      //   client.query('START TRANSACTION;');
      // },
    
      function(cb) {
        client.query(
         'INSERT INTO collaborateur SET id_collaborateur = ?, nom = ?, prenom = ?, qualification = ?', 
         ["", json_cv.lastname, json_cv.firstname, json_cv.titre],
         function(err, info) {
           collaborateur_id = info;
           if (err) log.warning(err);
           log.info("Inserted into collaborateur");
         }
        );
        cb(null, collaborateur_id);
      },

      function(data, callback) {
        for (var i = json_cv.langues.length - 1; i >= 0; i--){
          client.query(
           'INSERT INTO collaborateur_langue SET id_collaborateur = ?, id_langue = (SELECT id_langue FROM langue WHERE libelle = ?), niveau = ?', 
           [collaborateur_id, json_cv.langues[i].langue, json_cv.langues[i].niveau],
           function(err) {
             log.info("Tried inserting into collaborateur_langue");
             if (err) log.warning(err);
             log.info("Inserted into collaborateur_langue");
           }
          );
        }
      // },
      // function(callback) {
      //   client.query('COMMIT;');
      }
    ]);
    
    cb(null);
  }
};


module.exports = EtlWorkQueue;