require 'json'
require "embulk/parser/jsonl.rb"

module Embulk
  module Guess
    # $ embulk guess -g "jsonl" partial-config.yml

    class Jsonl < TextGuessPlugin
      Plugin.register_guess("jsonl", self)

      def guess_text(config, sample_text)
        return {} unless config.dig("parser", "type") == "jsonl"

        rows = []

        newline_type = config.fetch("parser", {}).fetch("newline", "CRLF")
        newline_char = newline_character(newline_type)
        sample_text.split(newline_char).each do |line|
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

      private

      def newline_character(newline_type)
        case newline_type
        when "CRLF"
          "\r\n"
        when "LF"
          "\n"
        when "CR"
          "\r"
        else
          "\r\n"
        end
      end
    end
  end
end
