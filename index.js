
var Log = require('coloured-log');
var util = require('util');

var config = require('./config');
var log = new Log(config.logLevel);

var SqlClient = require('mysql').Client;
var EtlWorkQueue = require('./lib/EtlWorkQueue');
var DirectoryWatcher = require('./lib/DirectoryWatcher');



log.info('Starting DocxResumeEtl...');

var sqlClient = new SqlClient(config.mysql);
var etlWorkQueue = new EtlWorkQueue(config.extractTransformBinary, sqlClient, config.concurrency, log);
var enqueueFile = function(file) {
	etlWorkQueue.addFile(file);
}
var directoryWatcher = new DirectoryWatcher(config.queueDirectory, enqueueFile, log);

directoryWatcher.run();