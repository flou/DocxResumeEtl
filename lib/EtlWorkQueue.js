var util = require('util');
var _ = require('underscore');
var async = require('async');
var spawn = require('child_process').spawn;
var exec = require('child_process').exec;

var titleize = function(str){
	var arr = str.split(' '),
    word;
    
	//if (arr.length <= 1) return str;

	for (var i=0; i < arr.length; i++) {
    	word = arr[i].split('');
        if(typeof word[0] !== 'undefined') word[0] = word[0].toUpperCase();
        i+1 === arr.length ? arr[i] = word.join('') : arr[i] = word.join('') + ' ';
	}
	return arr.join('');
}

var bind = function (fun, thisArg) {
	return  function () {
		return fun.apply(thisArg || null, arguments);
	};
};

var EtlWorkQueue = function(extractTransformBinary, sqlClient, concurrency, log) {
	var self = this;
	
	this._log = log;
	this._extractTransformBinary = extractTransformBinary;
	this._sqlClient = sqlClient;
	
	this._sqlClient.connect();
	
	this._fileQueue = async.queue(bind(this._processFile, this), concurrency);
	this._fileQueue.drain = function() {
		self._log.debug('All items have been processed, gimme more !');
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
				
				var doc = JSON.parse(stdout);
				self._log.debug('Extracted : ' + util.inspect(doc));
				cb(null, doc);
			
			} else {
				cb('Process terminated with code : ' + code + ' and signal : ' + signal);
			}
		});
		
	},
	
	_load: function(doc, cb) {

		this._log.info('Starting load...');
		
		var client = this._sqlClient;
		var self = this;
		var jsonResume = doc;
    	var collaborateurId = 0;

    	async.waterfall([
			function(cb) {
				self._log.info('Starting transaction...');
				
				client.query('START TRANSACTION', [], cb);
				
			},
			function(info, cb) {
				self._log.info('Inserting collaborator...');
				
        		client.query(
         			'INSERT INTO collaborateur SET nom = ?, prenom = ?, qualification = ?', 
         			[jsonResume.lastname, jsonResume.firstname, jsonResume.titre],
					cb
        		);
        	},
			
			function(info, cb) {
				collaborateurId = info.insertId;
				
				self._log.info('Collaborator <#'+ collaborateurId +'> inserted');
				
				cb(null);
			},
			
			
			function(cb) {
				self._findOrInsertSecteurs(jsonResume.experience_sectorielle, cb);
			},
			
			function(secteurIdx, cb) {
				
				self._log.info('Association des secteurs au collaborateur');
				
				async.forEachSeries(secteurIdx, function(item, cb) {
					client.query(
						'INSERT INTO collaborateur_secteur SET id_collaborateur = ?, id_secteur = ?', 
						[collaborateurId, item],
						cb
					);
				}, cb);
				
			},
			
			function(cb) {
				self._findOrInsertFamillesMetier(jsonResume.competences_fonctionnelles, cb);
			},
			
			function(secteurIdx, cb) {
				
				self._log.info('Association des compétences fonctionnelles au collaborateur');
				
				async.forEachSeries(secteurIdx, function(item, cb) {
					client.query(
						'INSERT INTO competences_fonctionnelles SET id_collaborateur = ?, id_famille_metier = ?', 
						[collaborateurId, item],
						cb
					);
				}, cb);
				
			},
			
			function(cb) {
				self._findOrInsertTechnologies(jsonResume.competences_techniques, cb);
			},
			
			function(secteurIdx, cb) {
				
				self._log.info('Association des compétences techniques au collaborateur');
				
				async.forEachSeries(secteurIdx, function(item, cb) {
					client.query(
						'INSERT INTO competences_techniques SET id_collaborateur = ?, id_technologie = ?', 
						[collaborateurId, item],
						cb
					);
				}, cb);
				
			},

			function(cb) {
				
				self._log.info('Inserting collaborator languages...');
				
				async.forEachSeries(jsonResume.langues, function(item, cb) {
					client.query(
						'INSERT INTO collaborateur_langue SET id_collaborateur = ?, id_langue = (SELECT id_langue FROM langue WHERE libelle = ?), niveau = ?', 
						[collaborateurId, item.langue, item.niveau],
						cb
					);
				}, cb);
      		},

			function(cb) {
				
				self._log.info('Insertion domaines de competence...');
				
				async.forEachSeries(jsonResume.domaines_de_competences, function(item, cb) {
					client.query(
						'INSERT INTO domaine_competences SET id_collaborateur = ?, libelle = ?',
						[collaborateurId, item],
						cb
					);
				}, cb);
      		},

			function(cb) {
				
				self._log.info('Insertion diplomes et certifications...');
				
				async.forEachSeries(jsonResume.diplomes, function(item, cb) {
					client.query(
						'INSERT INTO diplome_certification SET id_collaborateur = ?, etablissement = ?, annee = ?, libelle = ?',
						[collaborateurId, item.institut, item.annee, item.diplome],
						cb
					);
				}, cb);
      		},

			function(cb) {
				
				self._log.info('Insertion des missions...');
				
				async.forEachSeries(jsonResume.historique_carriere, function(item, cb) {
					self._insertMission(collaborateurId, item, cb);
				}, cb);
				
			}
    	
		], function(err) {
			
			if (err) {
				self._log.debug(err.message);
				self._log.warning('Rollback transaction');
				client.query('ROLLBACK', [], cb);
				return;
			}
			
			self._log.info('Commiting transaction...');
			client.query('COMMIT', [], cb);
		});

	},

	_insertMission: function(collaborateurId, mission, cb) {
		var self = this;
		var client = this._sqlClient;
		
		var missionClientId = null;
		var missionTechnologiesIdx = null;
		var missionId = null;
		
		async.waterfall([
			
			function(cb) {
				self._findOrInsertClient([mission.entreprise], cb);
			},
			
			function(clientIdx, cb) {
				
				missionClientId = clientIdx[0];
				var techs = mission.environnement_tech;
				
				self._findOrInsertTechnologies(techs, cb);
			},
			
			function(technologieIdx, cb) {
				
				missionTechnologiesIdx = technologieIdx;
				
				self._log.info('Création de la mission...');
				self._log.debug('Début : ' + mission.periode.debut);
				self._log.debug('Fin : ' + mission.periode.fin);
				
				client.query(
         			'INSERT INTO mission SET id_collaborateur = ?, id_client = ?, date_debut = ?, date_fin = ?, responsabilite = ?, projet = ?, descriptif = ?', 
         			[collaborateurId, missionClientId, mission.periode.debut, mission.periode.fin, mission.responsabilite, mission.projet, mission.fonction],
					cb
        		);
			},
			
			function(info, cb) {
				missionId = info.insertId;
				self._log.info('Mission <#'+ missionId +'> inserted');
				
				cb(null);
			}, 
			
			function(cb) {
			
				async.forEachSeries(missionTechnologiesIdx, function(item, cb) {
					client.query(
						'INSERT INTO mission_technologie SET id_mission = ?, id_technologie = ?', 
						[missionId, item],
						cb
					);
				}, cb);
				
			},
			
		], function(err) {
			cb(err);
		});
	},
	
	_findOrInsertClient: function(dataArray, cb) {
		return this._findOrInsert('client', 'nom', 'id_client', dataArray, cb);
	},
	_findOrInsertTechnologies:function(dataArray, cb) {
		return this._findOrInsert('technologie', 'tagname', 'id_technologie', dataArray, cb);
	},
	_findOrInsertSecteurs:function(dataArray, cb) {
		return this._findOrInsert('secteur', 'libelle', 'id_secteur', dataArray, cb);
	},
	_findOrInsertFamillesMetier:function(dataArray, cb) {
		return this._findOrInsert('famille_metier', 'libelle', 'id_famille_metier', dataArray, cb);
	},
	_findOrInsert:function(tableName, valueFieldName, idFieldName, dataArray, cb) {

		var self = this;
		var client = this._sqlClient;
		var returnedForeignKeys = [];
		
		dataArray = dataArray.map(function(e) { return titleize(e.trim()); });
		
		client.query(
			'SELECT * FROM '+ tableName +' WHERE '+ valueFieldName +' IN (' + dataArray.map(function() {return '?'}).join(',') + ')',
			dataArray.slice(),
			function(err, results, fields) {

			    if (err) {
					cb(err);
			    	return;
				}

				if (dataArray.length > 0) {
					self._log.info('Insertion de '+dataArray.length+' '+tableName);

					async.forEachSeries(dataArray, function(item, cb) {
						self._log.info('Insertion '+tableName+' : "' + item +'"');
						client.query('INSERT INTO '+tableName+' SET '+valueFieldName+' = ?', [titleize(item)], function(err, info) {
							if (err) {
								cb(err);
								return;
							}

							returnedForeignKeys.push(info.insertId);
							cb(null);
						});
					}, function(err) {
						if (err) {
							cb(err);
							return;
						}

						cb(null, returnedForeignKeys)
					});

				} else {
					cb(null, returnedForeignKeys);
				}
			}
		)
		.on('row', function(row) {
			self._log.info(tableName+' "'+ row[valueFieldName] + '" existe déjà en base');
			dataArray = _.without(dataArray, row[valueFieldName]);
			returnedForeignKeys.push(row[idFieldName]);
		});

	}
};


module.exports = EtlWorkQueue;