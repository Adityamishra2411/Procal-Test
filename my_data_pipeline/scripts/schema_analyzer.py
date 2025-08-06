import pandas as pd
import numpy as np
import os
import json
import sys

class CsvSchemaAnalyzer:
    def __init__(self, source_csv_path):
        if not os.path.exists(source_csv_path):
            raise FileNotFoundError(f"Source file not found at '{source_csv_path}'")
        self.source_df = pd.read_csv(source_csv_path, low_memory=False)
        self._convert_data_types()
        self.inferred_schema = self._infer_schema()

    def _convert_data_types(self):
        for column in self.source_df.columns:
            if self.source_df[column].dtype == 'object':
                converted_series = pd.to_numeric(self.source_df[column], errors='coerce')
                if converted_series.isnull().sum() / len(converted_series) < 0.5:
                    self.source_df[column] = converted_series
            
    def _infer_schema(self):
        inferred_schema = {}
        for column in self.source_df.columns:
            inferred_schema[column] = {
                'dtype': str(self.source_df[column].dtype),
                'unique_count': self.source_df[column].nunique(dropna=False),
                'total_rows': len(self.source_df),
                'value_counts': None,
                'min_max': None
            }
            if inferred_schema[column]['dtype'].startswith('object'):
                if inferred_schema[column]['unique_count'] > 0 and inferred_schema[column]['unique_count'] < 100:
                    inferred_schema[column]['value_counts'] = self.source_df[column].value_counts(dropna=False).to_dict()
            
            if pd.api.types.is_numeric_dtype(self.source_df[column]):
                numeric_series = self.source_df[column].dropna()
                if not numeric_series.empty:
                    inferred_schema[column]['min_max'] = {
                        'min': numeric_series.min(),
                        'max': numeric_series.max()
                    }
                else:
                    inferred_schema[column]['min_max'] = None
        return inferred_schema

if __name__ == '__main__':
    if len(sys.argv) != 2:
        print(json.dumps({"error": "Usage: python schema_analyzer.py <source_csv_path>"}), file=sys.stderr)
        sys.exit(1)
    
    source_csv_path = sys.argv[1]
    try:
        analyzer = CsvSchemaAnalyzer(source_csv_path)
        print(json.dumps(analyzer.inferred_schema)) # Output JSON to stdout
    except Exception as e:
        print(json.dumps({"error": str(e)}), file=sys.stderr)
        sys.exit(1)
