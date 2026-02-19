#!/usr/bin/env python3
# Copyright (C) 2023 Genome Surveillance Unit/Genome Research Ltd.

import argparse
from collections import defaultdict
import csv
import json
import re


class KelchMutationCaller:
    """
    Uses genotypes call at kelch13 loci from supplied genotype files to call per-sample kelch13 mutations.
    Uses supplied genotypes files, kelch13 regions, codon key and kelch reference sequence file.
    Writes these per-sample kelch13 variant calls to a single output file.

    Rationale behind implementation:
    1) Mutant alleles at kelch13 loci genotypes can be used to identify amino acid substitution.
    2) These identified non-synonymous mutations may be important markers of drug resistance e.g. reduced artimisin suspectility.
    """

    def __init__(
        self,
        genotype_files,
        out_file_name,
        config,
        kelch_reference_file,
        codon_key_file,
    ):
        self.genotype_file_list = genotype_files
        self.output_file_name = out_file_name
        self.config = config
        self.kelch_reference_file = kelch_reference_file
        self.codon_key_file = codon_key_file
        self.complementary_bases = {"A": "T", "T": "A", "G": "C", "C": "G"}

    def call_kelch_variants(self):
        """
        Calls kelch13 variants across a specified region for all samples in all supplied genotype files:
        1) Reads in necessary files -> codon key file, config and kelch reference file.
        2) For all the kelch13 rows for each sample ID in all of the genotype files the following is done
        3) The variant is called -> wild type, missing or any number of non-synonymous mutations
        4) The sample ID and the variant are written to the output file.
        """

        # Read in kelch regions from the config
        with open(self.config) as file:
            kelch_config = json.load(file).get("grc_kelch13")
            kelch_regions = kelch_config.get("kelch13_regions")
            minimum_kelch13_WT_coverage = kelch_config.get(
                "minimum_kelch13_WT_coverage"
            )

        # Read in codon key file
        with open(self.codon_key_file) as file:
            codon_key = {}
            for row in file:
                row = row.split()
                # Codon as key and amino acid as value
                codon_key[row[3]] = row[4]

        # Read in kelchReference.txt - key is each gl1, gl2 and gl3 column from every row
        with open(self.kelch_reference_file) as file:
            kelch_reference = {}
            for row in csv.DictReader(file, delimiter="\t"):
                for gl in [row.get("GL1"), row.get("GL2"), row.get("GL3")]:
                    kelch_reference[gl] = dict(row)

        # Open output file and write header
        output_file = open(self.output_file_name, "w")
        file_writer = csv.DictWriter(
            output_file, delimiter="\t", fieldnames=["ID", "kelch13"]
        )
        file_writer.writeheader()

        # Iterate over the supplied per-sample genotype files
        for genotype_file_path in self.genotype_file_list:

            # Get rows from this genotype file that are between the specified kelch13 loci
            kelch_genotype_file_rows = self._get_kelch_genotype_file_rows(
                genotype_file_path, kelch_regions
            )

            # Iterate over all of the sample IDs in this genotype file
            for ID in kelch_genotype_file_rows.keys():

                # Get all genotype file row for this sample ID
                per_sample_kelch_genotype_file_rows = kelch_genotype_file_rows.get(ID)

                # Determine this sample's variant
                variant = self._determine_sample_variant(
                    per_sample_kelch_genotype_file_rows,
                    kelch_reference,
                    minimum_kelch13_WT_coverage,
                    codon_key,
                )

                # Write variant(s) and sample ID to output file
                variant = " ".join(variant) if isinstance(variant, list) else variant
                row = {"ID": str(ID), "kelch13": variant}
                file_writer.writerow(row)

        output_file.close()

    def _get_kelch_genotype_file_rows(self, genotype_file_path, kelch_regions):
        """
        Returns a list created from the the provided genotype file.
        Only adds genotype file rows that have co-ordinates between the specified kelch loci
        """
        genotype_file_dict = defaultdict(list)
        genotype_file = open(genotype_file_path, newline="")
        for row in csv.DictReader(genotype_file, delimiter="\t"):
            row_chromosome = row[str("Chr")]
            row_locus = row[str("Loc")]
            region = kelch_regions.get(row_chromosome)

            if region != None:
                start = region.get("Start")
                end = region.get("End")

                # If this row is not a valid kelch13 locus then ignore it
                if row_locus >= start and row_locus <= end:
                    key = row.get("ID")
                    genotype_file_dict[key].append(dict(row))
        genotype_file.close()
        return genotype_file_dict

    def _determine_sample_variant(
        self,
        kelch_genotype_file_rows,
        kelch_reference,
        minimum_kelch13_WT_coverage,
        codon_key,
    ):
        """
        Determines the variant for a given sample:
        1) Checks for non-synonymous mutations at all non-reference alleles for a given kelch13 locus.
        2) Returns all non-synonymous mutations found across kelch13 for this sample.
        3) Returns wild-type if reference allele is present.
        4) If no non-synonymous mutations are found, then a coverage check is performed.
        5) Coverage check determines whether variant is missing or wild type.
        """
        total = 0
        number_of_calls = 0
        mutations = []
        WT_alleles_present = 1

        # Iterate over the kelch13 rows in the retrieved the genotype file
        for row in kelch_genotype_file_rows:

            # Get locus and associated kelch reference row
            locus = row.get("Loc")
            kelch_reference_row = kelch_reference.get(locus)

            # Get called and reference alleles at this position from this sample
            alleles = row.get("Gen")
            reference_allele = self._get_reference_allele(locus, kelch_reference_row)
            total += 1

            # If the genotype call is missing then go to the next position
            if alleles == "-":
                continue
            else:
                # If genotype call isn't missing, count it so we know the coverage in the end
                number_of_calls += 1

            # If there are no mutant alleles then go to the next position
            if alleles == reference_allele:
                continue

            # Look at each of the alleles for this position and check for non-synonymous mutations
            has_reference = 0
            non_synonymous_mutations = []
            alleles = alleles.split(",")
            for allele in alleles:

                # If there is a reference allele, then add wild type as one of the alleles.
                if allele == reference_allele:
                    has_reference = 1
                    continue
                if allele == "-":
                    continue

                # If this allele causes an amino acid substitution then update non-synonymous mutations list
                amino_acid_substitution = self._check_for_amino_acid_substitution(
                    allele, locus, kelch_reference_row, codon_key
                )
                if amino_acid_substitution != None:
                    non_synonymous_mutations.append(amino_acid_substitution)

            # If any of the alleles for this position cause non-synonymous mutations then retain these mutations
            if len(non_synonymous_mutations) > 0:
                mutations.extend(non_synonymous_mutations)
                if has_reference == 0:
                    WT_alleles_present = 0

        # If non-synonymous mutations were found across kelch13, then return them
        if len(mutations) > 0:
            # Reverse order the mutations by amino acid position
            mutations_dict = {int(re.sub(r"\D", "", item)): item for item in mutations}
            positions = sorted(list(mutations_dict.keys()), reverse=True)
            mutations = [mutations_dict.get(position) for position in positions]

            # Add wild type if they were all heterozygous
            if WT_alleles_present:
                mutations.insert(0, "WT")
        else:
            coverage = number_of_calls / total if total > 0 else 0
            # If coverage reaches threshold and no non-synonymous mutations found then return wild type
            if coverage >= minimum_kelch13_WT_coverage:
                return "WT"
            # Otherwise return missing
            else:
                return "-"
        return mutations

    def _get_reference_allele(self, locus, kelch_reference_row):
        """
        Returns the reference allele for a given locus from a kelch reference row:
        1) Checks which of 3 location columns in the matching kelch row match the locus.
        2) Gets the nucleotide relating to the locus matched location column.
        3) Complements and returns this base.
        """
        genomic_location_columns = {"GL1": "n1", "GL2": "n2", "GL3": "n3"}
        for location, nucleotide in genomic_location_columns.items():
            # Get key column which matches locus
            if kelch_reference_row.get(location) == locus:
                # Retrieve and complement associated base
                base = kelch_reference_row.get(nucleotide)
                complemented_base = self.complementary_bases.get(base)
                return complemented_base

    def _check_for_amino_acid_substitution(
        self, allele, locus, kelch_reference_row, codon_key
    ):
        """
        Determines whether amino acid substitution has taken place:
        1) Uses the reference codon to get the original amino acid.
        2) Creates a mutation codon using the supplied allele and gets the new amino acid.
        3) Checks if these amino acids match.
        4) If they don't match then the resulting non-synonymous mutation name is assembled and returned.
        """
        # Get the complement of the mutant nucleotide
        complement_allele = self.complementary_bases.get(allele)

        # Get the reference codon and the corresponding amino acid
        amino_acid_position = kelch_reference_row.get("AApos")
        original_amino_acid = kelch_reference_row.get("AA")

        # Get the mutated codon and from there the corresponding amino acid
        mutated_codon = self._assemble_mutated_codon(
            locus, complement_allele, kelch_reference_row
        )

        new_amino_acid = codon_key.get(mutated_codon)

        # Only consider the mutation if nonsynonymous - amino acid alteration
        if new_amino_acid != original_amino_acid and new_amino_acid != None:
            # Assemble amino acid substitution name: wild type amino acid + location + mutant amino acid
            return f"{original_amino_acid}{amino_acid_position}{new_amino_acid}"

    def _assemble_mutated_codon(self, locus, complement_allele, kelch_reference_row):
        """
        Assembles and returns a codon by:
        1) Checking which of 3 location columns in the matching kelch row match the locus.
        2) Getting all of the nucleotide columns
        3) Updating the nucleotide relating to the locus matched location column to be the complemented allele.
        4) From this assembling all of the nucleotide columns into a codon before returning it.
        """
        genomic_location_columns = ["GL1", "GL2", "GL3"]
        codon_bases = [kelch_reference_row.get(base) for base in ["n1", "n2", "n3"]]

        for index, location in enumerate(genomic_location_columns):
            # Assemble mutated codon from bases in matched kelch reference row

            # At the codon nucleotide where the genomic location matches the locus
            if kelch_reference_row.get(location) == locus:

                # Replace the nucleotide at this index with the complement of mutant allele
                codon_bases[index] = complement_allele

                # Return mutated codon
                mutated_codon = "".join(codon_bases)
                return mutated_codon


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--genotype_files",
        "-i",
        help="List of per-sample genotype files to call kelch variants from.",
        required=True,
        type=str,
        nargs="+",
        default=[],
    )

    parser.add_argument(
        "--output_file",
        "-o",
        help="Name for the output kelch13 mutations file. Default is kelch13_mutation_calls.txt.",
        type=str,
        default="kelch13_mutation_calls.txt",
    )

    parser.add_argument(
        "--config",
        "-c",
        help="Config JSON file containing the kelch13 regions to look between.",
        required=True,
        type=str,
    )

    parser.add_argument(
        "--kelch_reference_file",
        "-r",
        help="Kelch reference file. Tab-separated file containing a breakdown of the kelch13 reference sequence, codon structure and amino translation.",
        required=True,
        type=str,
    )

    parser.add_argument(
        "--codon_key_file",
        "-k",
        help="Path to the codon key file for translating nucleotide codons into associated amino acids.",
        required=True,
        type=str,
    )

    args = parser.parse_args()
    kelch_variants = KelchMutationCaller(
        args.genotype_files,
        args.output_file,
        args.config,
        args.kelch_reference_file,
        args.codon_key_file,
    )
    kelch_variants.call_kelch_variants()
