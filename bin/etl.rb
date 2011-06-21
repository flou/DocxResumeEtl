#!/usr/bin/env ruby -wKU

MONTHS = {
  /janvier/i   => "01",
  /f.vrier/i   => "02", 
  /mars/i      => "03",
  /avril/i     => "04",
  /mai/i       => "05",
  /juin/i      => "06",
  /juillet/i   => "07",
  /ao.t/i      => "08",
  /septembre/i => "09",
  /octobre/i   => "10",
  /novembre/i  => "11",
  /d.cembre/i  => "12"
}


class ETL
  EXTRACT_SCRIPT = File.join(File.dirname(File.expand_path(__FILE__)) , 'extract.sh')

  def initialize(cv_file)
    @entreprises     = Array.new
    @periodes        = Array.new
    @responsabilites = Array.new
    @fonctions       = Array.new
    @projets         = Array.new
    @envtech         = Array.new

    @cv              = Hash.new

    raw_cv = %x{"#{EXTRACT_SCRIPT}" "#{cv_file}"}
    @cv    = to_json(raw_cv)
  end


  def to_array(raw_cv)
    ary = []

    raw_cv.split(/\n/).each do |line|
      if !line.empty?
        line = line.split(/:/, 2)
        ary.push line
      end
    end

    cpt = 0
    ary.each do |item|
      value = item[1]
      case item[0]
      when "Historique"
        if cpt % 2 == 1
          @periodes.push value.strip
        else
          @entreprises.push value.strip
        end
        cpt += 1
      when "projet"
        @projets.push value.strip
      when "fonction"
        @fonctions.push value.strip
      when "environnement_technique"
        @envtech.push value.strip
      when "responsabilite"
        responsabilite = cleanup(value)
        @responsabilites.push responsabilite.strip
      end
    end
    @experiences = cpt / 2
  end


  def to_json(raw_cv)
    to_array(raw_cv)
    raw_cv.split(/\n/).each do |line|
      if !line.empty?
        line = line.split(/:/, 2)
    	  key = line[0].sub(/\p{Z}$/, "")
        @cv[key] = line[1]
      end
    end
    reformat
    return @cv
  end


  def reformat
    @cv.each do |item|
      value = item[1]
      if value != nil && value != ""
        value = cleanup(value)
      end
    end

    %w{experience_sectorielle competences_fonctionnelles
       competences_techniques domaines_de_competences}.each do |category|
      format_category(category)
    end

    format_career_history
    format_nom        unless @cv["nom"] == nil
    format_langues    unless @cv["langues"] == nil
    format_diplomes   unless @cv["diplomes"] == nil
    format_formations unless @cv["formations"] == nil
    remove_synthese
  end
  

  def remove_synthese
    if @cv.has_key? "synthese"
      @cv.delete "synthese"
    end
  end


  def format_nom
    fullname = @cv["nom"].split(/\s/)
    firstname, lastname = [], []
    fullname.each do |part|
      if part.upcase == part
        lastname.push part
      else
        firstname.push part
      end
    end
    @cv["lastname"]  = lastname.join(' ')
    @cv["firstname"] = firstname.join(' ')
    @cv.delete "nom"
  end

  
  ##
  # Formatte une chaine sous la forme MM/AAAA
  def to_date(str)
    origin_str = str
    MONTHS.each do |month|
      if str.match(/\p{Letter}*/).to_s.downcase.match(month[0])
        str.sub!(/\p{Letter}*/, month[1])
        str.sub!(/\s/, "/")
      end
    end
    if str == origin_str
      str.sub!(/\p{L}*\s/, "")
    end
    return str
  end

  
  ##
  # Formatte toute la catégorie Historique de carriere du CV
  def format_career_history
    historique = []
    (0..@experiences - 1).each do |i|
      experience = {}

      periode = @periodes[i].scan(/\p{Letter}*\s\d{4,}/)
            
      if periode.size == 2 # 2 dates: debut-fin
        debut = to_date(periode[0])
        fin   = to_date(periode[1])
      elsif periode.size == 1 # une seule date
        debut = to_date(periode[0])
      end

      experience["entreprise"]         = @entreprises[i]
      experience["periode"]            = {}
      experience["periode"]["debut"]   = debut
      experience["periode"]["fin"]     = fin if fin
      experience["projet"]             = @projets[i]
      experience["fonction"]           = @fonctions[i]
      experience["responsabilite"]     = @responsabilites[i]
      experience["environnement_tech"] = @envtech[i]
      historique.push experience
    end
    @cv["historique_carriere"] = historique
    %w{
      Historique
      projet
      fonction
      responsabilite
      environnement_technique}.each do |cat|
      @cv.delete cat
    end
  end


  def cleanup(text)
    text.gsub!("&amp;", "&")
    text.gsub!(/\s*\|\s*/, "|")
    text.gsub!(/^\s*\|\s*/, "") # clean up at beginning of line
    text.gsub!(/\s*\|\s*$/, "") # clean up at end of line
    return text
  end


  def format_formations
    form = cleanup @cv["formations"]
    ary = []
    form.split(/\|/).each { |element| ary.push element }
    @cv["formations"] = ary
  end


  def format_diplomes
    form = cleanup @cv["diplomes"]
    ary = []
    form.split(/\|/).each do |element| 
      diplome = {}
      md = element.match(/^(.*)\s*\((\d{4,})\)\s*:\s*(.*)$/)
      if md != nil
        diplome["institut"] = md[1].strip
        diplome["annee"]    = md[2]
        diplome["diplome"]  = md[3]
      else
        diplome["institut"] = element
      end
      ary.push diplome
    end
    @cv["diplomes"] = ary
  end


  def format_langues
    ary = []
    @cv["langues"].split(/\|/).each do |lang|
      lang = lang.split(/:/)
      langue = {}
      langue["langue"] = lang[0].strip
      langue["niveau"] = lang[1].strip
      ary.push langue
    end
    @cv["langues"] = ary
  end


  def format_category(cv_category)
    content = cleanup @cv[cv_category]
    ary = []
    content.split(/\|/).each { |element| ary.push element }
    @cv[cv_category] = ary
  end
  
  
  def to_s
    p @cv
  end
end

cv = ETL.new ARGV[0]
cv.to_s
