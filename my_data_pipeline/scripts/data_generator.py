import pandas as pd
import numpy as np
import os
import random
from faker import Faker
import json
import sys

class CsvDataGenerator:
    def __init__(self, inferred_schema):
        self.inferred_schema = inferred_schema
        self.fake = Faker('en_US')
        self.type_mappings = {
            'object': self.fake.word,
            'int64': self.fake.random_digit_not_null,
            'float64': self.fake.pyfloat,
            'datetime64[ns]': self.fake.date_time_this_century
        }

    def _get_faker_function(self, column_name, dtype, details):
        if 'email' in column_name.lower():
            return self.fake.email
        if 'name' in column_name.lower() or 'director' in column_name.lower() or 'cast' in column_name.lower():
            return self.fake.name
        if 'title' in column_name.lower() or 'description' in column_name.lower():
            return self.fake.sentence
        if 'country' in column_name.lower() or 'rating' in column_name.lower() or 'listed_in' in column_name.lower() or 'type' in column_name.lower():
            if details['value_counts']:
                choices_with_nan = list(details['value_counts'].keys())
                weights_with_nan = list(details['value_counts'].values())
                valid_choices = [c for c in choices_with_nan if pd.notna(c)]
                valid_weights = [weights_with_nan[i] for i, c in enumerate(choices_with_nan) if pd.notna(c)]
                if valid_choices:
                    return lambda: random.choices(valid_choices, weights=valid_weights, k=1)[0]
                else:
                    return self.fake.word
        if 'year' in column_name.lower() and details['min_max']:
            min_year = int(details['min_max']['min']) if pd.notna(details['min_max']['min']) else 1900
            max_year = int(details['min_max']['max']) if pd.notna(details['min_max']['max']) else 2025
            return lambda: self.fake.year(min_year=min_year, max_year=max_year)
        
        return self.type_mappings.get(dtype, self.fake.word)

    def generate_data(self, num_rows):
        generated_data = {column: [] for column in self.inferred_schema.keys()}
        
        for _ in range(num_rows):
            for column, details in self.inferred_schema.items():
                dtype = details['dtype']
                faker_func = self._get_faker_function(column, dtype, details)
                try:
                    generated_value = faker_func()
                    generated_data[column].append(generated_value)
                except Exception:
                    generated_data[column].append(np.nan)
                    
        synthetic_df = pd.DataFrame(generated_data)
        for column, details in self.inferred_schema.items():
            dtype_str = details['dtype']
            if dtype_str.startswith('int') or dtype_str.startswith('float'):
                synthetic_df[column] = pd.to_numeric(synthetic_df[column], errors='coerce')
            elif dtype_str == 'datetime64[ns]':
                synthetic_df[column] = pd.to_datetime(synthetic_df[column], errors='coerce')
        
        return synthetic_df

if __name__ == '__main__':
    if len(sys.argv) != 3:
        print(json.dumps({"error": "Usage: python data_generator.py <inferred_schema_json> <num_records>"}), file=sys.stderr)
        sys.exit(1)
    
    try:
        inferred_schema = json.loads(sys.argv[1])
        num_records = int(sys.argv[2])
        
        generator = CsvDataGenerator(inferred_schema)
        synthetic_df = generator.generate_data(num_records)
        
        output_file_path = f"data/synthetic_data_{num_records}.csv" # Save to data folder
        synthetic_df.to_csv(output_file_path, index=False)
        print(json.dumps({"generated_file_path": output_file_path})) # Output JSON to stdout
    except Exception as e:
        print(json.dumps({"error": str(e)}), file=sys.stderr)
        sys.exit(1)