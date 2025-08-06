import pandas as pd
import numpy as np
import json
import sys
import os

class DataValidator:
    def __init__(self, dataframe):
        self.df = dataframe
        self.validation_report = {}

    def run_validation(self, rules):
        total_records = len(self.df)
        self.validation_report = {'total_records': total_records, 'validation_results': {}}
        
        for column, column_rules in rules.items():
            results = {}
            if column not in self.df.columns:
                results['column_exists'] = False
                self.validation_report['validation_results'][column] = results
                continue
            
            results['column_exists'] = True
            
            if 'dtype' in column_rules:
                expected_dtype = column_rules['dtype']
                is_correct_dtype = str(self.df[column].dtype).startswith(expected_dtype)
                if not is_correct_dtype and (expected_dtype.startswith('int') or expected_dtype.startswith('float')):
                    is_correct_dtype = pd.api.types.is_numeric_dtype(self.df[column])
                results['dtype_check'] = {'passed': is_correct_dtype, 'actual_dtype': str(self.df[column].dtype)}
            
            if 'range' in column_rules:
                min_val, max_val = column_rules['range']
                numeric_column = pd.to_numeric(self.df[column], errors='coerce').dropna()
                if not numeric_column.empty:
                    out_of_range_count = len(numeric_column[~numeric_column.between(min_val, max_val, inclusive='both')])
                    results['range_check'] = {
                        'passed': out_of_range_count == 0,
                        'out_of_range_count': out_of_range_count
                    }
                else:
                    results['range_check'] = {'passed': True, 'message': 'No numeric data to check range.'}
            
            self.validation_report['validation_results'][column] = results
        
        return self.validation_report

if __name__ == '__main__':
    if len(sys.argv) != 3:
        print(json.dumps({"error": "Usage: python data_validator.py <generated_csv_path> <inferred_schema_json>"}), file=sys.stderr)
        sys.exit(1)
    
    try:
        generated_csv_path = sys.argv[1]
        inferred_schema = json.loads(sys.argv[2])
        
        if not os.path.exists(generated_csv_path):
            print(json.dumps({"error": f"Generated file not found at '{generated_csv_path}'"}), file=sys.stderr)
            sys.exit(1)

        synthetic_df = pd.read_csv(generated_csv_path, low_memory=False)
        validator = DataValidator(synthetic_df)
        
        validation_rules = {}
        for col, details in inferred_schema.items():
            validation_rules[col] = {'dtype': details['dtype']}
            min_max_details = details.get('min_max')
            if min_max_details and pd.notna(min_max_details.get('min')) and pd.notna(min_max_details.get('max')):
                validation_rules[col]['range'] = (min_max_details['min'], min_max_details['max'])

        report = validator.run_validation(validation_rules)
        print(json.dumps(report)) # Output JSON to stdout
    except Exception as e:
        print(json.dumps({"error": str(e)}), file=sys.stderr)
        sys.exit(1)