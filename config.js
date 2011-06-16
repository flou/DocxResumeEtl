var _ = require('underscore');
var Log = require('coloured-log');

var applicationEnv = process.env.NODE_ENV || 'development';
var configs = {};

var defaultConfig = {
	logLevel:Log.DEBUG,
	queueDirectory: __dirname + '/tests/queue',
	concurrency:1,
	extractTransformBinary: __dirname + '/bin/etl.rb'
}

configs.development = defaultConfig;
configs.production = _.extend({}, defaultConfig, {
	logLevel:Log.INFO,
});

module.exports = configs[applicationEnv];