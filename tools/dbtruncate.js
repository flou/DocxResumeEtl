
var Log = require('coloured-log');
var util = require('util');
var csv = require('csv');
var async = require('async');

var config = require('../config');
var log = new Log(config.logLevel);

var Client = require('mysql').Client;
var client = new Client(config.mysql);

var tableToTruncate = [
	'collaborateur_langue', 
	'collaborateur_secteur', 
	'competences_fonctionnelles', 
	'competences_techniques', 
	'diplome_certification', 
	'domaine_competences',
	'mission_technologie',
	'mission',
	'collaborateur',
	'client'
];

log.info('Starting DbTruncate...');

client.connect();
purgeDb(function() {
	log.info('DbTruncate Finished');
	client.end();
});

function purgeDb(cb) {

	log.info("Deleting all data...");
	async.forEachSeries(tableToTruncate, function(item, cb) {
		log.info("Deleting table : " + item + ' ...');
		client.query("TRUNCATE " + item, cb);
	}, function(err) {
		if (err) {
			log.warning(err);
			cb(err);
			return;
		}
	
		log.info("Done deleting");
		cb(null);
	});

}
