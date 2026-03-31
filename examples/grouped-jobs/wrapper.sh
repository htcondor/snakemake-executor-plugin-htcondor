#!/bin/bash
# This is required since Snakemake and HTCondor typically don't share a filesystem.
# For more details about Job Wrappers, see the job-wrapper example folder.
set -e

export HOME=$(pwd)

snakemake "$@"
