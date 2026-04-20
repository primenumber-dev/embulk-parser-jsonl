# Register Java GuessPlugin directly
Embulk::JavaPlugin.register_guess(
  "jsonl", "org.embulk.parser.jsonl.JsonlGuessPlugin",
  File.expand_path("../../../../classpath", __FILE__)
)
