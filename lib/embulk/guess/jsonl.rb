require 'json'
require "embulk/parser/jsonl.rb"

module Embulk
  module Guess
    # $ embulk guess -g "jsonl" partial-config.yml

    class Jsonl < LineGuessPlugin # TODO should use GuessPlugin instead of LineGuessPlugin
      Plugin.register_guess("jsonl", self)

      def guess_lines(config, sample_lines)
        #return {} unless config.fetch("parser", {}).fetch("type", "jsonl") == "jsonl"

        rows = []

        columns = {}
        sample_lines.each do |line|
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
