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
	}
}

EtlWorkQueue.prototype = {
	
	addFile: function(file, cb) {
		this._fileQueue.push(file, cb || function() {});
	},
	
	_processFile: function(file, cb) {

		var self = this;
		var log = this._log;

		log.info('Processing file : ' + file);
		
		async.waterfall([
			function(cb) {
				cb(null, file);
			},
			bind(this._extractTransform, this),
			bind(this._load, this)
		], function(err) {
			
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
		
		cb(null);
	}
};


module.exports = EtlWorkQueue;