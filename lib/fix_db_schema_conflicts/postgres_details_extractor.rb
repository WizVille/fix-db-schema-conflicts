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
        exttracted_values = extract_types_components(enum)
        stream.puts exttracted_values
        types_in_file(exttracted_values)
      end

      if @pg_composite_types.present?
        @pg_composite_types.each do |composite|
          exttracted_values = extract_types_components(composite)
          stream.puts extract_types_components(exttracted_values)
          types_in_file(exttracted_values)
        end
      end
      stream.puts("  SQL")
    end

    def create_functions(stream)
      @pg_functions.each do |row|
        function_content, function_name = extract_function_components(row)

        stream.write(<<~SQL)
          execute <<~EOSQL
            #{function_content}
          EOSQL
        SQL
       functions_in_file(function_name, function_content)
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

    private

    def extract_function_components(row)
      # Extract function components
      function_name = row.name
      arguments = row.arguments
      return_type = row.return_type
      language_name = row.language_name

      # Ensure function body doesn't have trailing semicolons
      function_body = row.body.strip.sub(/;+\z/, '')

      # Check if the function body starts with 'BEGIN'
      starts_with_begin = function_body.match?(/^\s*BEGIN/i)

      if starts_with_begin
        function_content = <<~SQL
        CREATE OR REPLACE FUNCTION #{function_name}(#{arguments})
          RETURNS #{return_type} AS $$
            #{function_body};
          $$ LANGUAGE #{language_name} #{row.volatility};
        SQL
      else
        function_content = <<~SQL
        CREATE OR REPLACE FUNCTION #{function_name}(#{arguments})
            RETURNS #{return_type} AS $$
              BEGIN
                #{function_body};
              END;
          $$ LANGUAGE plpgsql #{row.volatility};
        SQL
      end
      [function_content, function_name]
    end

    def extract_types_components(row)
      <<~SQL
        DO $$
        BEGIN
          IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = '#{row.name}') THEN
            CREATE TYPE #{row.name} AS (#{row.attributes});
          END IF;
        END
        $$;
      SQL
    end

    def functions_in_file(file_name,function_content)
      function_file_path = Rails.root.join('db', 'functions')
      file_name = function_file_path.join("#{file_name}.sql")
      FileUtils.mkdir_p function_file_path unless function_file_path.exist?
      File.open(file_name, "w") {} unless File.exist?(file_name)
      File.open(file_name, 'a') do |file|
        function_content = function_content.gsub(/\n+/, "\n")
                                   .gsub(/^\s+/m, '')
                                   .strip
        formatted_sql = function_content.gsub(/(?=\b(AS|BEGIN|END|LANGUAGE)\b)/, "\n")
        file.write(formatted_sql + ";")
        file.puts
        file.puts
      end
    end

    def types_in_file(type_content)
      type_file_path = Rails.root.join('db', 'types')
      file_name = type_file_path.join("types.sql")
      FileUtils.mkdir_p type_file_path unless type_file_path.exist?
      File.open(file_name, "w") {} unless File.exist?(file_name)
      File.open(file_name, 'a') do |file|
        type_content = type_content.gsub(/\n+/, "\n")
                                     .gsub(/^\s+/m, '')
                                     .strip
        formatted_sql = type_content.gsub(/(?=\b(AS|BEGIN|END|LANGUAGE|CREATE)\b)/, "\n")
        file.write(formatted_sql + ";")
        file.puts
        file.puts
      end
    end
  end
end
