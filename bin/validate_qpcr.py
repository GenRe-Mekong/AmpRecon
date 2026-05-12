#!/usr/bin/env python3
# Copyright (C) 2026 GenRe-Mekong Core Team.

import sys
import pandas as pd
import logging
from argparse import ArgumentParser

# Setup logging to handle errors appropriately
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')


def parse_args():
    parser = ArgumentParser()
    parser.add_argument("--qpcr-input", "-i", help="path to tab-delimited qPCR input file")
    parser.add_argument("--force-fill-columns", "-f", help="create missing columns with blank values", action="store_true")
    parser.add_argument("--output", "-o", help="path to output file", default="validated_qpcr.tsv")
    return parser.parse_args()

class QPcrStandardizer:
    VALID_SPECIES_MAP = {
        "PF": "pf", "PV": "pv", "PK": "pk", "PM": "pm", "PO": "po", "-": "-"
    }
    
    AMP_MAPPING = {
        "NEG": "-",
        "MISSING": "-",
        "COPY": "Amplified"
    }

    def __init__(self, file_path, fill_columns=False):
        self.qpcr = self.load_tsv(file_path)
        self.fill_columns = fill_columns

    def load_tsv(self, file_path):
        """Loads TSV and performs case-insensitive header matching."""
        try:
            df = pd.read_csv(file_path, sep='\t')
            # Normalize headers to lowercase for internal processing
            df.columns = [c.strip().lower() for c in df.columns]
            return df
        except Exception as e:
            logging.error(f"Failed to load {file_path}: {e}")
            sys.exit(1)


    def standardize_amp(self, series):
        """Standardizes Amplification values (mdr1, pm23)."""
        # Convert to string, strip whitespace, and handle case-insensitivity
        s = series.astype(str).str.strip().str.upper()
        
        # Apply mapping
        s = s.replace(self.AMP_MAPPING)
        
        # Check for unexpected values
        valid_set = {"-", "WT", "AMPLIFIED"}
        unexpected_mask = ~s.isin(valid_set)
        unexpected_values = s[unexpected_mask].unique()
        
        if len(unexpected_values) > 0:
            expected_values = list(self.AMP_MAPPING.keys()) + ["WT"]
            logging.error(f"Non-standard AMP values found: {unexpected_values}")
            logging.error(f"Expected values are: {expected_values}")
            # Show indices if less than 10 unexpected
            unexpected_indices = s[unexpected_mask].index.tolist()
            if len(unexpected_indices) <= 10:
                logging.error(f"Unexpected values found at indices: {unexpected_indices}")
                sys.exit(1)
            else:
                logging.error(f"Total unexpected values: {len(unexpected_indices)} (showing first 10 indices: {unexpected_indices[:10]})")
                sys.exit(1)
        return s.replace({"AMPLIFIED": "Amplified"}) # Fix casing for output

    def standardize_species(self, series):
        """Standardizes Species values."""
        missing_set = {"NEG", "MISSING", "NAN", "-"}

        s = series.astype(str).str.strip().str.upper()
        s = s.apply(lambda x: "-" if x in missing_set else x)

        # Accumulate unexpected values and their indices
        unexpected_records = []

        def clean_row(val, idx):
            # Handle both "/" and "," delimiters
            parts = val.replace("/", ",").split(",")
            out_parts = []
            
            for p in [p.strip() for p in parts if p.strip()]:
                if p in self.VALID_SPECIES_MAP:
                    out_parts.append(self.VALID_SPECIES_MAP[p])
                else:
                    unexpected_records.append({"index": idx, "value": val})
            
            return ",".join(out_parts)

        # Process each row with its index
        result = pd.Series([clean_row(val, idx) for idx, val in s.items()], index=s.index)
        
        # Report accumulated unexpected values
        if len(unexpected_records) > 0:
            expected_values = list(self.VALID_SPECIES_MAP.keys())
            unique_unexpected = list(set([r["value"] for r in unexpected_records]))
            
            logging.error(f"Non-standard species values found: {unique_unexpected}")
            logging.error(f"Expected species codes are: {expected_values}")
            
            indices = [r["index"] for r in unexpected_records]
            if len(indices) <= 10:
                logging.error(f"Unexpected values found at indices: {indices}")
                sys.exit(1)
            else:
                logging.error(f"Total unexpected values: {len(indices)} (showing first 10 indices: {indices[:10]})")
                sys.exit(1)
        
        return result

    def validate_header(self):
        if "sample_id" not in self.qpcr.columns:
            logging.error("The required header 'sample_id' is missing.")
            sys.exit(1)

        expected_header = ['sample_id','mdr1-qpcr', 'pm23-qpcr', 'species-qpcr'] 
        missing_header  = [ header for header in expected_header if header not in self.qpcr.columns]

        if (self.fill_columns):
            for header in missing_header:
                self.qpcr[header] = "-"
            # remove duplicates after filling missing columns   
            self.qpcr = self.qpcr.drop_duplicates(subset=['sample_id']) 
            # reset missing header after filling
            missing_header = [] 
            
        if  len(missing_header) > 0:
            missing_header_str = ", ".join(missing_header)
            logging.error(f"Expected header: {expected_header}")
            logging.error(f"The required header is missing: {missing_header_str}")
            sys.exit(1)

    def process(self, output_path=None):
        """Main logic to process a single TSV file and merge."""
        if self.qpcr is None:
            logging.error("No data to process.")
            sys.exit(1)

        self.validate_header()

        processed_data = pd.DataFrame()

        processed_data['ID'] = self.qpcr['sample_id'].astype(str).str.strip()
        processed_data['mdr1-qpcr'] = self.standardize_amp(self.qpcr['mdr1-qpcr'])
        processed_data['pm23-qpcr'] = self.standardize_amp(self.qpcr['pm23-qpcr'])
        processed_data['species-qpcr'] = self.standardize_species(self.qpcr['species-qpcr'])

        if output_path is None:
            output_path = "validated_qpcr.tsv"
        self.save(processed_data, output_path)

    def save(self, df, output_path):
        """Saves final TSV."""
        df.to_csv(output_path, sep='\t', index=False)
        logging.info(f"Saved data to {output_path}")

def main():
    args = parse_args() 
    qpcr_standardizer = QPcrStandardizer(args.qpcr_input, args.force_fill_columns)
    qpcr_standardizer.process(output_path=args.output)

if __name__ == "__main__":
    main()