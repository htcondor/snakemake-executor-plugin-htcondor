# Changelog

## [0.2.0](https://github.com/htcondor/snakemake-executor-plugin-htcondor/compare/v0.1.2...v0.2.0) (2026-02-23)

### Features

- **Container universe**: Added support for submitting jobs in the HTCondor container universe.
- **Non-shared filesystem support**: Full file transfer support when no shared filesystem exists between the submit and execute nodes, including automatic transfer of the Snakefile, job wrappers, config files, and scripts ([#11](https://github.com/htcondor/snakemake-executor-plugin-htcondor/pull/11), [#14](https://github.com/htcondor/snakemake-executor-plugin-htcondor/pull/14), [#30](https://github.com/htcondor/snakemake-executor-plugin-htcondor/pull/30)).
- **Partial/split filesystem support**: Specify HTCondor shared filesystem prefixes to support partially-shared filesystem configurations ([#6](https://github.com/htcondor/snakemake-executor-plugin-htcondor/pull/6)).
- **HTCondor v2 Python bindings**: Upgraded to the `htcondor2` bindings, resolving credential errors and adapting to iterator/list API changes ([#2](https://github.com/htcondor/snakemake-executor-plugin-htcondor/pull/2)).
- **Custom ClassAds**: Allow users to specify custom HTCondor ClassAd attributes via Snakemake job resources.
- **Job naming**: Jobs are now named in the HTCondor queue as `<rule>-<jobid>` for easier identification.
- **Resource request enhancements**: Added explicitly-suffixed resource parameters (e.g. `cpus_ex`) to better support grouped/provisioned jobs; standardized resource parsing with a `_set_resources()` helper ([#37](https://github.com/htcondor/snakemake-executor-plugin-htcondor/pull/37), [#38](https://github.com/htcondor/snakemake-executor-plugin-htcondor/pull/38)).
- **Job event log monitoring**: Jobs are now monitored using HTCondor job event logs instead of polling `condor_history`/schedd, with a schedd/history query as fallback for cases where logs cannot be read ([#42](https://github.com/htcondor/snakemake-executor-plugin-htcondor/pull/42)).

### Bug Fixes

- Fixed custom ClassAd quoting: values are now properly double-quoted in the submit description.
- Fixed config file handling: config files are no longer incorrectly flattened when transferred ([#14](https://github.com/htcondor/snakemake-executor-plugin-htcondor/pull/14)).
- Fixed path handling for Snakefiles, job wrappers, and scripts ([#11](https://github.com/htcondor/snakemake-executor-plugin-htcondor/pull/11)).
- Credential generation is now wrapped in a try block for more robust error handling ([#2](https://github.com/htcondor/snakemake-executor-plugin-htcondor/pull/2)).
- Allow `max_retries=0` to correctly disable job retries ([#21](https://github.com/htcondor/snakemake-executor-plugin-htcondor/pull/21)).
- Use the full Python executable path when not using a job wrapper, preventing environment resolution failures ([#30](https://github.com/htcondor/snakemake-executor-plugin-htcondor/pull/30)).
- Fixed `gpus_minimum_memory` resource parsing failure caused by an extraneous space ([#37](https://github.com/htcondor/snakemake-executor-plugin-htcondor/pull/37)).

### Documentation

- Moved documentation from `docs/` into `README.md` ([#45](https://github.com/htcondor/snakemake-executor-plugin-htcondor/pull/45)).
- Added detailed usage examples for non-shared and partially-shared filesystem configurations.

### CI / Code Quality

- Added pre-commit configuration with `black` and `flake8` ([#29](https://github.com/htcondor/snakemake-executor-plugin-htcondor/pull/29)).
- Integrated pre-commit checks into GitHub Actions CI ([#34](https://github.com/htcondor/snakemake-executor-plugin-htcondor/pull/34)).
- Expanded unit test suite covering file transfer utilities, resource parsing (`_set_resources`), and job event log monitoring.

## [0.1.2](https://github.com/jannisspeer/snakemake-executor-plugin-htcondor/compare/v0.1.1...v0.1.2) (2024-03-25)

### Bug Fixes

- logger messages ([5b66926](https://github.com/jannisspeer/snakemake-executor-plugin-htcondor/commit/5b6692668ac54b5267c35856f6de3503c43c22e5))

## [0.1.1](https://github.com/jannisspeer/snakemake-executor-plugin-htcondor/compare/v0.1.0...v0.1.1) (2024-03-25)

### Documentation

- improved documentation ([#8](https://github.com/jannisspeer/snakemake-executor-plugin-htcondor/issues/8)) ([f8d0744](https://github.com/jannisspeer/snakemake-executor-plugin-htcondor/commit/f8d07440cd4a40d5eca00c35f1d094694681e5cf))

## 0.1.0 (2024-03-23)

### Bug Fixes

- job status method ([#6](https://github.com/jannisspeer/snakemake-executor-plugin-htcondor/issues/6)) ([0ee1b47](https://github.com/jannisspeer/snakemake-executor-plugin-htcondor/commit/0ee1b474879e032bc9a53b120aafbf08bdb605af))
- Removing single quotes from job argument ([#5](https://github.com/jannisspeer/snakemake-executor-plugin-htcondor/issues/5)) ([f721f0d](https://github.com/jannisspeer/snakemake-executor-plugin-htcondor/commit/f721f0d20ca5d821ade21be8649e8b2a7d5bf3e8))
- solved several bugs to obtain first running version ([#3](https://github.com/jannisspeer/snakemake-executor-plugin-htcondor/issues/3)) ([5cdfdd2](https://github.com/jannisspeer/snakemake-executor-plugin-htcondor/commit/5cdfdd2937206d46d2f615ac5a3c4ae20d440907))
