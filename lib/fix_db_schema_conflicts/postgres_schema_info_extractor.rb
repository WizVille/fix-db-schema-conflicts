# frozen_string_literal: true
require 'delegate'
require 'fix-db-schema-conflicts'
require 'rails'

module FixDBSchemaConflicts
  class PostgresSchemaInfoExtractor
    def initialize(connection)
      @connection = connection
    end

    def pg_functions
      query = <<-SQL
        SELECT 
            proname AS function_name,
            pg_get_function_arguments(pg_proc.oid) AS arguments,
            pg_get_function_result(pg_proc.oid) AS return_type,
            CASE
                WHEN prosrc IS NOT NULL THEN prosrc
                ELSE 'Invalid Function Body'
            END AS function_body,
            CASE provolatile
                WHEN 'i' THEN 'IMMUTABLE'
                WHEN 's' THEN 'STABLE'
                WHEN 'v' THEN 'VOLATILE'
                ELSE 'UNKNOWN'
            END AS volatility,
            pg_language.lanname AS language_name
        FROM pg_proc
        JOIN pg_namespace ON pg_namespace.oid = pg_proc.pronamespace
        JOIN pg_language ON pg_language.oid = pg_proc.prolang
        WHERE pg_language.lanname IN ('sql', 'plpgsql')
          AND pg_namespace.nspname NOT IN ('pg_catalog', 'information_schema')
          AND proname NOT LIKE 'pg_%'
        ORDER BY function_name;
      SQL

      @connection.execute(query).map do |row|
        OpenStruct.new(
          name: row['function_name'],
          arguments: row['arguments'],
          return_type: row['return_type'],
          body: row['function_body'],
          volatility: row['volatility'],
          language_name: row['language_name']
        )
      end
    end

    def fetch_enum_types
      query = <<-SQL
        SELECT t.typname AS type_name,
               string_agg(quote_literal(e.enumlabel), ', ') AS enum_values
        FROM pg_type t
        JOIN pg_enum e ON t.oid = e.enumtypid
        WHERE t.typcategory = 'E'
        GROUP BY t.typname;
      SQL

      @connection.execute(query).map do |row|
        OpenStruct.new(
          name: row['type_name'],
          enum_values: row['enum_values']
        )
      end
    end

    def fts_configurations
      query = <<-SQL
        SELECT
          nspname AS schema_name,
          cfgname AS configuration_name
        FROM
          pg_ts_config
        JOIN
          pg_namespace ON pg_ts_config.cfgnamespace = pg_namespace.oid
        ORDER BY
          schema_name, configuration_name;
      SQL

      @connection.execute(query).map do |row|
        OpenStruct.new(schema: row['schema_name'], name: row['configuration_name'])
      end
    end
  end
end
