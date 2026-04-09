#!/usr/bin/env python3
# Copyright (C) 2023 Genome Surveillance Unit/Genome Research Ltd.

import pandas as pd
import argparse

parser = argparse.ArgumentParser(
    prog="grc_assemple.py",
    description="""
        This script mergea an arbitrary set of grcs/tsv files
        """,
)

# inputs for write_mccoil_in mode
parser.add_argument(
    "-grcs_in",
    nargs="+",
    help="""
    Path(s) to barcode tsv file for a batch of samples
""",
    default=None,
    required=True,
)

parser.add_argument(
    "-grc_out_name",
    help="""
    name of the grc output (default = final.grc)
""",
    default="final.grc",
)

args = {k: v for k, v in vars(parser.parse_args()).items()}


# TODO sanity checks

# Create an empty DataFrame to hold the merged data
merged_data = pd.DataFrame()

# Iterate through each TSV file and merge with the existing data
for file in args["grcs_in"]:
    # Read the TSV file into a DataFrame
    df = pd.read_csv(file, sep="\t", index_col=0)
    # Merge the DataFrame with the existing data
    merged_data = pd.concat([merged_data, df], axis=1, sort=False)
    
# Drop two extra columns from kelch13 mutation caller if they exist
# Keep the original GRC format.
for col in ["who_cov_perc", "missing_validated_muts"]:
    if col in merged_data.columns:
        merged_data.drop(columns=[col], inplace=True, errors="ignore")


# fill NaN values with "-" and write final grc
# -- handle special columns --
# intify McCOIL columns (PS: min value for COI is 1, in this context 0 is NaN)

if "McCOIL" in merged_data.columns:
    merged_data["McCOIL"] = (
        pd.to_numeric(merged_data["McCOIL"], errors="coerce")
        .fillna(0)
        .astype(int)
        .replace(0, "-")
    )

# sort entries and fill nan values as "-"
merged_data.fillna("-").sort_values(by=["ID"]).to_csv(args["grc_out_name"], sep="\t")
