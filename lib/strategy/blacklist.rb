module DataAnon
  module Strategy
    class Blacklist < DataAnon::Strategy::Base
      def process_record(index, record)
        dest_record_map = {}

        record.attributes.each do |field_name, field_value|
          unless field_value.nil? || is_primary_key?(field_name)
            dest_record_map[field_name] = field_value
          end
        end

        @fields.each do |field, strategy|
          database_field_name = record.attributes.select { |k,v| k.downcase == field }.keys[0]
          field_value = record.attributes[database_field_name]
          unless field_value.nil? || is_primary_key?(database_field_name)
            field = DataAnon::Core::Field.new(database_field_name, field_value, index, record, @name)
            dest_record_map[database_field_name] = strategy.anonymize(field)
          end
        end

        dest_record = dest_table.new dest_record_map, without_protection: true
        @primary_keys.each do |key|
          dest_record[key] = record[key]
        end
        dest_record.save!
      end
    end
  end
end
