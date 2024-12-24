require 'delegate'

module FixDBSchemaConflicts
  module SchemaDumper
    class ConnectionWithSorting < SimpleDelegator
      def extensions
        __getobj__.extensions.sort
      end

      def columns(table)
        __getobj__.columns(table).sort_by(&:name)
      end

      def indexes(table)
        __getobj__.indexes(table).sort_by(&:name)
      end

      def foreign_keys(table)
        __getobj__.indexes(table).sort_by(&:name)
      end

      def triggers(trigger)
        puts "triggers in class"
        __getobj__.triggers(trigger).sort_by(&:name)
      end
    end

    def extensions(*args)
      with_sorting do
        super(*args)
      end
    end

    def tables(stream)
      specific = ENV['SCHEMA'] || if defined? ActiveRecord::Tasks::DatabaseTasks
        File.join(ActiveRecord::Tasks::DatabaseTasks.db_dir, 'specific.rb')
      else
        "#{Rails.root}/db/specific.rb"
      end

      if File.exist?(specific)
        stream.puts("\t" + File.read(specific).gsub("\n", "\n\t") + "\n\n")
      end

      super(stream)
    end

    def triggers(stream)
      triggers = ENV['SCHEMA'] || if defined? ActiveRecord::Tasks::DatabaseTasks
                                    File.join(ActiveRecord::Tasks::DatabaseTasks.db_dir, 'triggers.rb')
                                  else
                                    "#{Rails.root}/db/triggers.rb"
                                  end

      if File.exist?(triggers)
        stream.puts("\t" + File.read(triggers).gsub("\n", "\n\t") + "\n\n")
      end

      super(stream)
    end

    def table(*args)
      with_sorting do
        super(*args)
      end
    end

    def trigger(*args)
      puts "trigger"
      puts args
      with_sorting do
        super(*args)
      end
    end

    def with_sorting
      old, @connection = @connection, ConnectionWithSorting.new(@connection)
      result = yield
      @connection = old
      result
    end
  end
end

ActiveRecord::SchemaDumper.send(:prepend, FixDBSchemaConflicts::SchemaDumper)
