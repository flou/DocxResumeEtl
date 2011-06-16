
var Log = require('coloured-log');
var util = require('util');

var config = require('./config');
var log = new Log(config.logLevel);

var EtlWorkQueue = require('./lib/EtlWorkQueue');
var DirectoryWatcher = require('./lib/DirectoryWatcher');

log.info('Starting DocxResumeEtl...');


var etlWorkQueue = new EtlWorkQueue(config.extractTransformBinary, config.concurrency, log);
var enqueueFile = function(file) {
	etlWorkQueue.addFile(file);
}
var directoryWatcher = new DirectoryWatcher(config.queueDirectory, enqueueFile, log);

directoryWatcher.run();