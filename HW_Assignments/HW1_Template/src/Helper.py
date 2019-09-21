class Helper():
    @staticmethod
    # Return True if a list or dict is not None and not empty
    def is_empty(any_list_or_dict):
        if any_list_or_dict is None:
            return True

        if len(any_list_or_dict) == 0:
            return True

        return False

    @staticmethod
    # Return True if a key_fields is not None, not empty and has the same dimension as key_columns
    def are_key_fields_valid(key_fields, key_columns):
        if Helper.is_empty(key_fields):
            return False

        if len(key_fields) != len(key_columns):
            return False

        return True

    @staticmethod
    # Return True if new_record is not None and contains all the column keys of the table
    def is_new_record_valid(new_record, column_keys):
        if Helper.is_empty(new_record):
            return False

        if set(new_record.keys()) != set(column_keys):
            return False

        return True

    @staticmethod
    # Return True if all columns in template are subset of column_keys
    def is_template_valid(template, column_keys):
        return set(template.keys()).issubset(set(column_keys))

    @staticmethod
    # Return True if all fields in field_list are subset of column_keys
    def is_column_list_valid(column_list, column_keys):
        return set(column_list).issubset(set(column_keys))

    @staticmethod
    # Check if a row matches a template, return True if template is None/empty
    def matches_template(row, template):
        result = True
        if template is not None:
            for k, v in template.items():
                if v != row.get(k, None):
                    result = False
                    break

        return result

    @staticmethod
    # Build a template which contains key columns and their value from key_fields
    def convert_key_fields_to_template(key_fields, key_columns):
        template = {}
        for i in range(0, len(key_columns)):
            template[key_columns[i]] = key_fields[i]

        return template

    @staticmethod
    # From a template, build another template which contains only key columns and their value
    def extract_key_columns_and_values_rom_template(template, key_columns):
        key_values_dict = {}
        for i in range(0, len(key_columns)):
            key_values_dict[key_columns[i]] = template[key_columns[i]]

        return key_values_dict

    @staticmethod
    # Check if two list of dicts are the same, regardless of the order of the elements
    def compare_two_list_of_dicts(l1, l2):
        random_key = next(iter(l1[0]))

        sorted_l1 = sorted(l1, key=lambda k: k[random_key])
        sorted_l2 = sorted(l2, key=lambda k: k[random_key])

        return sorted_l1 == sorted_l2