
var Log = require('coloured-log');
var util = require('util');
var csv = require('csv');

var config = require('../config');
var log = new Log(config.logLevel);

var Client = require('mysql').Client;
var client = new Client(config.mysql);

var dataTechPath  = 'data/technologies_tags.csv';
var languagesPath = 'data/langues.csv';
var metiersPath   = 'data/metiers.csv';


log.info('Starting DbInit...');

client.connect();

csv()
.fromPath(dataTechPath)
.on('data', function(data, index){
 //La première ligne contient les colonne
 if (index == 0) return;
 if (data[2] < 15) return;
    log.info('Inserting #'+index+' '+JSON.stringify(data));
 client.query(
   'INSERT INTO technologie SET tagname = ?, count = ?',
   [data[1], data[2]],
   function(err) {
     if (err) log.warning(err);
     log.info('Inserted #'+index+' '+JSON.stringify(data));
   }
 );
})
.on('end',function(count){
    log.info('Inserted ' + count + ' lines');
})
.on('error',function(error) {
    log.warning(error.message);
 throw error;
});


csv()
.fromPath(languagesPath)
.on('data', function(data, index){
    log.info('Inserting #'+index+' '+JSON.stringify(data));
	client.query(
		'INSERT INTO langue SET id_langue = ?, libelle = ?',
		[index, data[0]],
		function(err) {
			if (err) log.warning(err);
			log.info('Inserted #'+index+' '+JSON.stringify(data));
		}
	);
})
.on('end',function(count){
    log.info('Inserted ' + count + ' lines');
})
.on('error',function(error) {
    log.warning(error.message);
	throw error;
});


log.info('DbInit Finished');
