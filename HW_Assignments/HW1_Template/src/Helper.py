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
    # Return True if new_record is not None and contains all the column keys of the table
    def is_new_record_valid(new_record, column_keys):
        if new_record is None:
            return False

        if len(new_record) == 0:
            return False

        if new_record.keys() != column_keys:
            return False

        return True

    @staticmethod
    # Check if two list of dicts are the same, regardless of the order of the elements
    def compare_two_list_of_dicts(l1, l2):
        random_key = next(iter(a[0]))

        sorted_l1 = sorted(l1, key=lambda k: k[random_key])
        sorted_l2 = sorted(l2, key=lambda k: k[random_key])

        return sorted_l1 == sorted_l2
