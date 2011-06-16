var util = require('util');
var stalker = require('stalker');

var DirectoryWatcher = function(dir, fileCallback, log) {
	this._dir = dir;
	this._fileCallback = fileCallback;
	this._log = log;
}

DirectoryWatcher.prototype = {
	run: function() {
		
		var log = this._log;
		var self = this;
		var docxExtentionFilter = function(path) {
			var ext = '.docx';
			path = path.toLowerCase();
			return path.length >= ext.length && path.substring(path.length - ext.length) === ext;
		};
		
		stalker.watch(this._dir, {buffer:1000}, function (err, files) {
			if (err) {
				log.warning(err);
			} else {
				files.forEach(function(file) {
					if (docxExtentionFilter(file)) {
						log.info('File added : ' + file);
						self._fileCallback(file);
					} else {
						log.debug('Filename invalid : ' + file);
					}
				});
			}
		});
		
		log.info('Watching queue directory : ' + this._dir);
	}
};


module.exports = DirectoryWatcher;