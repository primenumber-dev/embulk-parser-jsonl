require 'json'
require "embulk/parser/jsonl.rb"

module Embulk
  module Guess
    # $ embulk guess -g "jsonl" partial-config.yml

    class Jsonl < TextGuessPlugin # TODO should use GuessPlugin instead of LineGuessPlugin
      p "DEBUG Parser Jsonl"
      Plugin.register_guess("jsonl", self)

      def guess_text(config, sample_text)
        #return {} unless config.fetch("parser", {}).fetch("type", "jsonl") == "jsonl"

        rows = []

        columns = {}
        sample_text.split("\n").each do |line|
          next if line.strip.empty?
          rows << JSON.parse(line)
        end

        min_rows_for_guess = config.fetch("parser", {}).fetch("min_rows_for_guess", 4)
        return {} if rows.size < min_rows_for_guess

        if rows.empty?
          raise "SchemaGuess Can't guess schema from no records"
        end
        column_names = rows.map(&:keys).flatten.uniq
        samples = rows.to_a.map { |hash| column_names.map { |name| hash[name] } }

        columns = Embulk::Guess::SchemaGuess.from_array_records(column_names, samples).map do |c|
          column = { name: c.name, type: c.type }
          column[:format] = c.format if c.format
          column
        end
        parser_guessed = {"type" => "jsonl"}
        parser_guessed["columns"] = columns
        return {"parser" => parser_guessed}
      end
    end
  end
end
