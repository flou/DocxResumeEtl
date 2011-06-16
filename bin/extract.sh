#!/bin/bash
if [ ! -f "$1" ]; then
	echo "Error: File does not exist"
	exit 0;
fi
# Prenom Nom : Fonction
unzip -p "$1" word/header\*.xml | sed \
-e 's/<wp:posOffset>-[0-9]*<\/wp:posOffset>//g' \
-e 's/\(<w:pStyle w:val="CVConsultantName"\/>\)/nom:\1/g' \
-e 's/\(<w:pStyle w:val="CVConsultantJob"\/>\)/\==newline==titre:\1/g' \
-e 's/<[^>]\{1,\}>//g; s/[^[:print:]]\{1,\}//g' \
-e 's/\(==newline==\)/\
/g' \
-e '1d'
# Contenu du CV
unzip -p "$1" word/document.xml | \
sed \
-e 's/\(<w:pStyle w:val="CVBullet1"\/>\)/| \1/g' \
-e 's/\(<w:pStyle w:val="CVTabBullet1"\/>\)/| \1/g' \
-e 's/\(<w:pStyle w:val="CVTabBullet2"\/>\)/| \1/g' \
\
-e 's/\(<w:pStyle w:val="CVKeypointHdr"\/>\)/==newline==/g' \
-e 's/\(<w:pStyle w:val="CVTabHdr"\/>\)/==newline==/g' \
-e 's/\(<w:pStyle w:val="CVExperienceHeader"\/>\)/\1==newline==Historique:/g' \
-e 's/<w:t>\(Projet\)<\/w:t>/==newline==\1:/g' \
-e 's/<w:t>\(Fonction\)<\/w:t>/==newline==\1:/g' \
-e 's/<w:t>\(Responsabilité\)<\/w:t>/==newline==\1:/g' \
-e 's/<w:t>\(Environnement\ technique\)<\/w:t>/==newline==\1:/g' \
-e 's/>/>\
/g' \
-e 's/<[^>]\{1,\}>//g; s/[^[:print:]]\{1,\}//g' \
-e 's/\(Historique\ de\ carrière\)\(.*\)/==newline==historique_carriere:\2/g' \
-e 's/\(Formations\)\(.*\)/==newline==formations:\2/g' \
-e 's/\(Compétences\ techniques\)\(.*\)/==newline==competences_techniques:\2/g' \
-e 's/\(Compétences\ fonctionnelles\)\(.*\)/==newline==competences_fonctionnelles:\2/g' \
-e 's/\(Expérience\ sectorielle\)\(.*\)/==newline==experience_sectorielle:\2/g' \
-e 's/\(Domaines\ de\ compétences\)\(.*\)/==newline==domaines_de_competences:\2/g' \
-e 's/\(Langues\)\(.*\)/==newline==langues:\2/g' \
-e 's/\(Diplômes\ et\ certifications\)\(.*\)/==newline==diplomes:\2/g' \
-e 's/\(Synthèse\ de\ carrière\)\(.*\)/==newline==synthese\2/g' \
-e 's/\(==newline==\)/\
/g' \
-e '1d'
