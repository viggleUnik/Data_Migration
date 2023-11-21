

class QueryGenerator:

    @staticmethod
    def generate_insert_statement(input_data, table_name):

        query_list = []
        for index, row in input_data.iterrows():
            columns = ', '.join(row.index)

            values_list = []

            for val in row.values:
                formatted_val = f"'{val}'" if isinstance(val, str) else str(val)
                values_list.append(formatted_val)

            values = ', '.join(values_list)
            query = f"INSERT INTO {table_name} ({columns}) VALUES ({values})"
            query_list.append(query)

        return query_list

    @staticmethod
    def generate_delete_statement(table_name, condition_column, condition_value):

        # check type and generate strings for querry

        if isinstance(condition_value, (int, float)):
            condition_value_str = str(condition_value)
        else:
            condition_value_str = f"'{condition_value}'"

        query = f"DELETE FROM {table_name} WHERE {condition_column} = {condition_value_str}"

        return query

    @staticmethod
    def generate_update_statement(table_name, set_column, set_value, condition_column, condition_value):

        # check type and generate strings for querry
        if isinstance(set_value, (int, float)):
            set_value_str = str(set_value)
        else:
            set_value_str = f"'{set_value}'"

        if isinstance(condition_value, (int, float)):
            condition_value_str = str(condition_value)
        else:
            condition_value_str = f"'{condition_value}'"

        query = f"UPDATE {table_name} SET {set_column} = {set_value_str} WHERE {condition_column} = {condition_value_str}"

        return query
