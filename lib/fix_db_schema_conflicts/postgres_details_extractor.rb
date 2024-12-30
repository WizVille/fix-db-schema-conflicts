# frozen_string_literal: true

module FixDBSchemaConflicts
  class PostgresDetailsExtractor
    def initialize(connection)
      @connection = connection
      @pg_enum_types = @connection.fetch_enum_types
      @pg_functions = @connection.pg_functions
      @pg_fts_configurations = @connection.fts_configurations
    end

    def create_types(stream)
      stream.puts("  execute <<-SQL")
      @pg_enum_types.each do |enum|
        stream.puts "DO $$ BEGIN " \
                      "IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = '#{enum.name}') THEN " \
                      "CREATE TYPE #{enum.name} AS ENUM (#{enum.enum_values}); " \
                      "END IF; " \
                      "END $$;"
      end

      if @pg_composite_types.present?
        @pg_composite_types.each do |composite|
          stream.puts "DO $$ BEGIN " \
                        "IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = '#{composite.name}') THEN " \
                        "CREATE TYPE #{composite.name} AS (#{composite.attributes}); " \
                        "END IF; " \
                        "END $$;"
        end
      end
      stream.puts("  SQL")
    end

    def create_functions(stream)
      @pg_functions.each do |row|
        # Extract function components
        function_name = row.name
        arguments = row.arguments
        return_type = row.return_type
        language_name = row.language_name

        # Ensure function body doesn't have trailing semicolons
        function_body = row.body.strip.sub(/;+\z/, '')

        # Check if the function body starts with 'BEGIN'
        starts_with_begin = function_body.match?(/^\s*BEGIN/i)

        # Handle functions correctly based on whether they require a procedural block
        if starts_with_begin
          # PL/pgSQL function with BEGIN...END already in the body
          stream.write(<<~SQL)
            execute <<~EOSQL
              CREATE OR REPLACE FUNCTION #{function_name}(#{arguments})
              RETURNS #{return_type} AS $$
              #{function_body};
              $$ LANGUAGE #{language_name} #{row.volatility};
            EOSQL
          SQL

        else
          # Add BEGIN...END for procedural logic
          stream.write(<<~SQL)
            execute <<~EOSQL
              CREATE OR REPLACE FUNCTION #{function_name}(#{arguments})
              RETURNS #{return_type} AS $$
              BEGIN
              #{function_body};
              END;
              $$ LANGUAGE plpgsql #{row.volatility};
            EOSQL
          SQL
        end

      end
    end

    def create_fts_configurations(stream)
      unless @pg_fts_configurations.empty?
        stream.puts("  execute <<-SQL")
        @pg_fts_configurations.each do |fts_configuration|
          stream.puts "DO $$ BEGIN " \
                        "IF NOT EXISTS (SELECT 1 FROM pg_ts_config WHERE cfgname = '#{fts_configuration.name}') THEN " \
                        "CREATE TEXT SEARCH CONFIGURATION #{fts_configuration.name} (COPY = simple); " \
                        "END IF; " \
                        "END $$;"
        end
        stream.puts("  SQL")
      end
    end
  end
end
