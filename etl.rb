#!/usr/bin/env ruby -wKU

require 'json'
require 'mysql2'

class ETL

  def initialize(cv_file)
    @entreprises    = Array.new
    @periodes       = Array.new
    @responsabilite = Array.new
    @fonctions      = Array.new
    @projets        = Array.new
    @envtech        = Array.new

    @cv             = Hash.new

    raw_cv = %x{./extract.sh #{cv_file}}.to_s
    @cv    = to_json(raw_cv)
  end


  def to_array(raw_cv)
    ary = []

    raw_cv.split(/\n/).each do |line|
      line = line.split(/:/, 2)
      ary.push line
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
      line = line.split(/:/, 2)
  	  key = line[0]
      @cv[key] = line[1]
    end
    reformat
    return @cv
  end


  def reformat
    @cv.each do |item|
      value = item[1]
      value = cleanup(value) if value != nil && value != ""
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
      historique["experience_#{i}"]["entreprise"]     = @entreprises[i-1]
      historique["experience_#{i}"]["periode"]        = @periodes[i-1]
      historique["experience_#{i}"]["projet"]         = @projets[i-1]
      historique["experience_#{i}"]["fonction"]       = @fonctions[i-1]
      historique["experience_#{i}"]["responsabilite"] = @responsabilite[i-1]
      historique["experience_#{i}"]["envtech"]        = @envtech[i-1]
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
p cv # puts JSON.pretty_generate(cv.cv)

# result = RubyProf.stop
# Print a flat profile to text
# printer = RubyProf::FlatPrinter.new(result)
# printer.print

# begin
#   # client = Mysql2::Client.new(:host => "localhost", :username => "root")
#   # puts Mysql2::Client.default_query_options
# rescue Mysql2::Error => e
#   puts "Error code: #{e.errno}"
#   puts "Error message: #{e.error}"
#   puts "Error SQLSTATE: #{e.sqlstate}" if e.respond_to?("sqlstate")
# ensure
#   # disconnect from server
#   client.close if client
# end
