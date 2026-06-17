#!/usr/bin/env python3
# Copyright (C) 2025 GenRe-Mekong Core Team.

import argparse
import pandas as pd
import os
import logging

def setup_logger(verbose):
    """Set up the logging configuration."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    return logging.getLogger(__name__)

def main():
    # =============================================================================
    # Argument Parsing
    # =============================================================================
    parser = argparse.ArgumentParser(description="Process Kraken2 reports and call species.")
    parser.add_argument('--suffix', type=str, default='_report.txt', 
                        help='Suffix to remove from filename to extract the Sample ID.')
    parser.add_argument('--min_reads', type=int, default=5, 
                        help='Absolute minimum clade reads required to call a species.')
    parser.add_argument('--min_fraction', type=float, default=0.01, 
                        help='Minimum fraction of total reads required (e.g., 0.01 for 1%%).')
    parser.add_argument('--output', type=str, default='species_calls.csv', 
                        help='Name of the output CSV file.')
    parser.add_argument('--verbose', action='store_true', 
                        help='Enable verbose debug logging.')
    
    args = parser.parse_args()
    logger = setup_logger(args.verbose)

    logger.info(f"Starting species calling with thresholds: {args.min_reads} min reads, {args.min_fraction*100}% min fraction.")

    # =============================================================================
    # Configuration
    # =============================================================================
    taxid_dict = {
        "5833": "Pf", "5855": "Pv", "5858": "Pm", "5850": "Pk",
        "36330": "Po", "864141": "Po", "864142": "Po"
    }

    kraken_cols = ['perc', 'clade_reads', 'tax_reads', 'rank', 'taxid', 'name']
    all_calls = []
    files_processed = 0

    # =============================================================================
    # Process Files
    # =============================================================================
    # Gather all target files in current directory
    target_files = [f for f in os.listdir('.') if f.endswith(args.suffix)]
    
    if not target_files:
        logger.error(f"No files found ending with suffix: '{args.suffix}' in the current directory.")
        # Create empty output to prevent downstream pipeline crashes
        pd.DataFrame(columns=['ID', 'species-kraken', 'k2_total_reads']).set_index('ID').to_csv(args.output, sep='\t')
        return

    logger.info(f"Found {len(target_files)} report files to process.")

    for filename in target_files:
        sample_id = filename.replace(args.suffix, "")
        
        try:
            report = pd.read_csv(filename, sep='\t', header=None, names=kraken_cols)
            files_processed += 1
        except Exception as e:
            logger.error(f"Failed to read {filename}: {e}")
            continue
            
        report['taxid'] = report['taxid'].astype(str).str.strip()
        
        # Calculate Total Reads (Unclassified + Root)
        total_reads_series = report[report['taxid'].isin(['0', '1'])]['clade_reads']
        total_reads = total_reads_series.sum() if not total_reads_series.empty else 0
        
        if total_reads == 0:
            logger.warning(f"Skipping {sample_id}: 0 total reads found.")
            continue
        
        # Apply Thresholds
        passed = report[(report['clade_reads'] >= args.min_reads) & 
                        (report['clade_reads'] >= (total_reads * args.min_fraction))].copy()
        
        # FIX: Only keep the taxids that are explicitly in our dictionary.
        # This automatically filters out 'root', 'Eukaryota', genus-level, and unclassified IDs.
        valid_calls = [taxid_dict[t] for t in passed['taxid'] if t in taxid_dict]
        
        # Remove duplicates while preserving order
        unique_calls = list(dict.fromkeys(valid_calls))
        calls_str = ", ".join(unique_calls)
        
        all_calls.append({
            'ID': sample_id,
            'species-kraken': calls_str,
            'k2_total_reads': total_reads
        })

    # =============================================================================
    # Export Results
    # =============================================================================
    if all_calls:
        df_results = pd.DataFrame(all_calls)
        df_results.set_index('ID', inplace=True)
        df_results.to_csv(args.output, sep='\t')
        logger.info(f"Successfully processed {files_processed} files. Results saved to {args.output}")
    else:
        empty_df = pd.DataFrame(columns=['ID', 'species-kraken', 'k2_total_reads'])
        empty_df.set_index('ID', inplace=True)
        empty_df.to_csv(args.output, sep='\t')
        logger.warning(f"No samples passed thresholds. Empty TSV generated at {args.output}")

if __name__ == "__main__":
    main()