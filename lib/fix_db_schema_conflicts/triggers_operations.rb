# frozen_string_literal: true
require 'delegate'

module FixDBSchemaConflicts
  class TriggersOperations
    def initialize(connection)
      @connection = connection
      @reset_done = false
    end

    def triggers_creation(table_name, stream, *args)
      triggers = @connection.triggers(table_name)
      unless triggers.empty?
        stream.puts("  execute <<-SQL")
        triggers.each do |trigger|
          stream.puts("#{trigger.definition} ;")
          add_triggers_in_file(table_name, trigger.definition)
        end
        stream.puts("  SQL")
      end
    end

    def reset_triggers_folder
      return if @reset_done

      dir_path = Rails.root.join('app', 'triggers')
      FileUtils.rm_rf Dir.glob("#{dir_path}/*") if dir_path.present?
      @reset_done = true
    end

    private

    def add_triggers_in_file(file_name, trigger_content)
      triggers_path = Rails.root.join('db', 'triggers')
      FileUtils.mkdir_p triggers_path unless triggers_path.exist?
      file_path = triggers_path.join("#{file_name}.sql")
      File.open(file_path, "w") {} unless File.exist?(file_path)
      File.open(file_path, 'a') do |file|
        trigger_content = trigger_content.gsub(/\n+/, "\n") # Remove extra blank lines
                                         .gsub(/^\s+/m, '') # Trim leading spaces for each line
                                         .strip # Remove leading/trailing blank lines
        formatted_sql = trigger_content.gsub(/(?=\b(AFTER|FOR EACH ROW|WHEN|EXECUTE FUNCTION)\b)/, "\n")
        file.write(formatted_sql + ";")
        file.puts
        file.puts
      end
    end
  end
end
