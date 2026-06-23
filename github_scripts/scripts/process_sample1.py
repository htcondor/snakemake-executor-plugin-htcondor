#!/usr/bin/env python3
"""
Sample-specific script for testing Snakemake script transfers with wildcards.
This script is selected based on the {wildcards.sample} value in the script path.
Reads numbers from input file, computes statistics, writes to output.

This script imports from stats_helpers.py to test the htcondor_transfer_input_files feature.
"""

# The `snakemake` object is injected at runtime by Snakemake's script: directive,
# so it is undefined to a static linter.
# ruff: noqa: F821

# Import helper functions (this file must also be transferred!)
from stats_helpers import compute_statistics, format_statistics

# Snakemake provides these variables automatically:
# snakemake.input - input files
# snakemake.output - output files
# snakemake.wildcards - wildcards (contains 'sample' in this case)
# snakemake.params - parameters (if any)


def main():
    # Access the wildcard value
    sample_name = snakemake.wildcards.sample

    # Read numbers from input
    with open(snakemake.input[0], "r") as f:
        numbers = [int(line.strip()) for line in f if line.strip()]

    # Compute statistics using helper module
    stats = compute_statistics(numbers)

    # Format output using helper module
    output_text = format_statistics(stats, sample_name)

    # Add the raw numbers list
    output_text += f"\nNumbers: {numbers}\n"

    # Write results
    with open(snakemake.output[0], "w") as f:
        f.write(output_text)

    print(f"✓ Script executed successfully for {sample_name}!")
    print(f"  This script was loaded from: scripts/process_{sample_name}.py")
    print(
        "  Helper module: scripts/stats_helpers.py (transferred via htcondor_transfer_input_files)"
    )
    print(f"  Processed {stats['count']} numbers")
    print(f"  Sum: {stats['sum']}, Average: {stats['average']:.2f}")


if __name__ == "__main__":
    main()
