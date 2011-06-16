#!/usr/bin/env ruby -wKU

class ETL
  EXTRACT_SCRIPT = File.join(File.dirname(File.expand_path(__FILE__)) , 'extract.sh')

  def initialize(cv_file)
    @entreprises    = Array.new
    @periodes       = Array.new
    @responsabilite = Array.new
    @fonctions      = Array.new
    @projets        = Array.new
    @envtech        = Array.new

    @cv             = Hash.new

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
      when "Projet"
        @projets.push value.strip
      when "Fonction"
        @fonctions.push value.strip
      when "Environnement technique"
        @envtech.push value.strip
      when "Responsabilité"
        responsabilite = cleanup(value)
        @responsabilite.push responsabilite.strip
      end
    end
    @experiences = cpt / 2
  end


  def to_json(raw_cv)
    to_array(raw_cv)
    raw_cv.split(/\n/).each do |line|
      if !line.empty?
        line = line.split(/:/, 2)
    	  key = line[0]
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

    %w{
      Expérience\ sectorielle
      Compétences\ fonctionnelles
      Diplômes\ et\ certifications
      Compétences\ techniques
      Domaines\ de\ compétences}.each do |category|
      format_category(category)
    end

    format_career_history
    format_langues
    format_formations if @cv["Formations"] != nil
  end


  def format_career_history
    historique = {}
    (0..@experiences - 1).each do |i|
      historique["experience_#{i}"] = {}
      historique["experience_#{i}"]["entreprise"]     = @entreprises[i]
      historique["experience_#{i}"]["periode"]        = @periodes[i]
      historique["experience_#{i}"]["projet"]         = @projets[i]
      historique["experience_#{i}"]["fonction"]       = @fonctions[i]
      historique["experience_#{i}"]["responsabilite"] = @responsabilite[i]
      historique["experience_#{i}"]["envtech"]        = @envtech[i]
    end
    @cv["Historique de carrière"] = historique
    %w{
      Historique
      Projet
      Fonction
      Responsabilité
      Environnement\ technique}.each do |cat|
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
    form = @cv["Formations"]
    form = cleanup(form)
    ary = []
    form.split(/\|/).each { |element| ary.push element }
    @cv["Formations"] = ary
  end

  def format_langues
    langues = {}
    @cv["Langues"].split(/\|/).each do |lang|
      lang = lang.split(/:/)
      langues[lang[0]] = lang[1]
    end
    @cv["Langues"] = langues
  end

  def format_category(cv_category)
    content = cleanup(@cv[cv_category])
    ary = []
    content.split(/\|/).each { |element| ary.push element }
    @cv[cv_category] = ary
  end
  
  def to_s
    @cv
  end
end

cv = ETL.new ARGV[0]
p cv
