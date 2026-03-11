#!/usr/bin/env python3

import argparse
import pandas as pd
import os
import sys
import csv
import logging


class GrcCurator:
    def __init__(
        self,
        run_id,
        grc_type="txt",
        flagged_hom_mutations="kelch_hom_mut_flagged.tsv",
        removed_mutations="kelch_removed.tsv",
        raw_GRC_path=None,
        curated_grc_output_path=None,
        accepted_mutations_file=None,
    ):
        self.grc_type = grc_type
        # Tracking Files
        self.accepted_mutations = self.load_accepted_mutations_list(
            accepted_mutations_file
        )
        self.removed_mutations = f"{run_id}_{removed_mutations}"
        self.flagged_hom_mutations = f"{run_id}_{flagged_hom_mutations}"

        # Input GRC checks
        if not os.path.exists(raw_GRC_path):
            logging.error("No raw GRC file provided. Please provide a path to the raw GRC file.")
            sys.exit(1)
        else:
            self.raw_GRC_path = raw_GRC_path

        # Files for run
        if grc_type == "xlsx":
            self.raw_GRC_df = pd.read_excel(
                raw_GRC_path,
                "GRC",
                index_col="Sample Internal ID",
                converters={"COIL": str, "McCOIL": str},
            )
        else:
            self.raw_GRC_df = pd.read_csv(raw_GRC_path, sep="\t", index_col="ID")

        # Output files
        if curated_grc_output_path is None:
            self.curated_grc_output_path = f"{run_id}_GRC_curated.txt"

    def load_accepted_mutations_list(self, accepted_mutations_file):
        """Loads csv of accepted mutations into a list"""
        if not accepted_mutations_file:
            logging.error("No accepted mutations file provided. Please provide a file with the list of accepted mutations.")
            sys.exit(1)

        logging.info(f"Using {accepted_mutations_file} for list of accepted mutations")
        accepted_mutations = [
            line.strip("\n") for line in open(accepted_mutations_file)
        ]
        return accepted_mutations

    def curate_kelch_column(self):
        """Curate kelch column.

        Returns
        -------
        df : pandas.DataFrame
        """
        curated_kelch_mutations = []
        count_removed_mut = 0
        for index, row in self.raw_GRC_df.iterrows():
            kelch_mutations = row["Kelch"].split()
            kelch_mutations, removed_count = self.filter_mutation_list(
                kelch_mutations, index
            )
            count_removed_mut += removed_count
            kelch_mutations = " ".join(str(mut) for mut in kelch_mutations)
            curated_kelch_mutations.append(kelch_mutations)
        if count_removed_mut == 0:
            print("No heterozygous mutations outside of the accepted list were found.")
        curated_GRC = self.raw_GRC_df
        curated_GRC["Kelch"] = curated_kelch_mutations
        return curated_GRC

    def filter_mutation_list(self, mutations, sample):
        """Removes any het calls not in accepted list. Flags any hom calls not in accepted list.

        Parameters
        ----------
        mutations : list
            List of mutations belonging to the sample to be curated
        sample : str
            Sample ID that mutations belong to

        Returns
        -------
        mutations : list
            List of curated mutations
        count_removed_mut : int
            Count of the number of mutations removed from this samples mutations
        """
        count_removed_mut = 0
        mutation_list = mutations.copy()
        for mutation in mutation_list:
            if mutation not in self.accepted_mutations:
                # Hom
                if len(mutation_list) == 1:
                    print(
                        f"{sample} has a homozygous unknown mutation that needs to be investigated: {mutations}"
                    )
                    self.update_tsv(
                        self.flagged_hom_mutations,
                        [
                            self.raw_GRC_path,
                            sample,
                            mutation,
                        ],
                    )
                # Het
                else:
                    mutations.remove(mutation)
                    print(f"Removing {mutation} from {sample}")
                    self.update_tsv(
                        self.removed_mutations,
                        [
                            self.raw_GRC_path,
                            sample,
                            mutation,
                        ],
                    )
                    count_removed_mut += 1
        if len(mutations) == 0:
            mutations = ["-"]
        return mutations, count_removed_mut

    def update_tsv(self, file_path, text):
        if not os.path.exists(file_path):
            with open(file_path, "w") as out_file:
                tsv_writer = csv.writer(out_file, delimiter="\t")
                tsv_writer.writerow(
                    [
                            "Raw GRC File",
                            "Sample ID",
                            "Flagged Mutation",
                    ]
                )

        with open(file_path, "r") as out_file:
            file_reader = csv.reader(out_file, delimiter="\t")
            # skip first line of tsv file            
            next(file_reader)
            existing_tsv_lines = [list(row) for row in file_reader]

        # Duplicate lines not written to output TSV file
        if text not in existing_tsv_lines:
            with open(file_path, "a") as out_file:
                tsv_writer = csv.writer(out_file, delimiter="\t")
                tsv_writer.writerow(text)

    def convert_pd_to_csv(self, grc_df):
        """Covert pandas df to tsv file in output directory"""
        grc_df.to_csv(self.curated_grc_output_path, sep="\t")

    def convert_pd_to_xlsx(self, grc_df):
        """Covert pandas df to xlsx file in output directory with GRC2 and Barcodes sheets"""
        grc2_df = pd.read_excel(
            self.raw_GRC_path, "GRC2", index_col="Sample Internal ID"
        )
        barcodes_df = pd.read_excel(
            self.raw_GRC_path, "Barcodes", index_col="Sample Internal ID"
        )
        with pd.ExcelWriter(self.curated_grc_output_path) as writer:
            grc_df.to_excel(writer, sheet_name="GRC")
            grc2_df.to_excel(writer, sheet_name="GRC2")
            barcodes_df.to_excel(writer, sheet_name="Barcodes")


def curate_grc(run, input_grc, accepted_mutations_file=None):
    """Curate GRC. Removes any het calls not in accepted list. Flags any hom calls not in accepted list.

    Parameters
    ----------
    run : int
        ID for the run being curated.
    """
    curator = GrcCurator(run, grc_type="txt", raw_GRC_path=input_grc, accepted_mutations_file=accepted_mutations_file)
    curated_grc_df = curator.curate_kelch_column()
    # if curated_grc_df.grc_type == "xlsx":
    #     curator.convert_pd_to_xlsx(curated_grc_df)
    # else:
    curator.convert_pd_to_csv(curated_grc_df)
    print(f"Curated GRC placed in {curator.curated_grc_output_path}")

def get_args():
    parser = argparse.ArgumentParser(description="Curate GRC. Removes any het calls not in accepted list. Flags any hom calls not in accepted list.")
    parser.add_argument("--run_id" ,help="ID for the run being curated.")
    parser.add_argument("--raw_grc_path", help="Path to the raw GRC file to be curated.")
    parser.add_argument("--accepted_mutations_file", help="Path to a file with a list of accepted mutations, one per line.")
    return parser.parse_args()

if __name__ == "__main__":
    args = get_args()
    curate_grc(args.run_id, args.raw_grc_path, args.accepted_mutations_file)
