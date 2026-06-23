#!/bin/bash

rm -rf logs/ output/ data/ pipeline/ .snakemake/

# Clean up shared filesystem test outputs
rm -rf /staging/torture-test/
