class Helper():
    @staticmethod
    # Return True if new_record is not None and contains all the column keys of the table
    def are_key_fields_valid(key_fields, primary_keys):
        if key_fields is None:
            return False

        if len(key_fields) == 0:
            return False

        if set(key_fields) != set(primary_keys):
            return False

        return True

    @staticmethod
    # Return True if new_record is not None and contains all the column keys of the table
    def is_template_valid(template):
        if template is None:
            return False

        if len(template) == 0:
            return False

        return True

    @staticmethod
    # Return True if new_record is not None and contains all the column keys of the table
    def are_new_values_valid(new_values):
        if new_values is None:
            return False

        if new_values == 0:
            return False

        return True

    @staticmethod
    # Return True if new_record is not None and contains all the column keys of the table
    def is_new_record_valid(new_record, column_keys):
        if new_record is None:
            return False

        if len(new_record) == 0:
            return False

        if new_record.keys() != column_keys:
            return False

        return True
