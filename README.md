# JSON parser

- uses jq and python to process JSON data with undefined structure and nesting
- accepts target fieldnames and JSON input, processes line by line and outputs CSV
- enumerates duplicate fieldname values found in nested JSON objects
- uses 2-pass parsing to get all possible fieldnames in first pass
- handles unpredictable nesting and number of duplicate target fieldnames
- accepts target columns from CLI args, delimited by ", " (comma + space)

## requirements

- jq command line parser: https://github.com/stedolan/jq
- Python Fire CLI arguments generator: https://github.com/google/python-fire

## todo

- standardise terminology: 'fieldname'/'column', and 'row'/'line' etc
- re-think: can 2-pass strategy be bypassed without bottlenecking memory?
- re-think/improve row and row_path state management and object copying
- fieldnames pass could use a slimmer gen_process_lines() function
- improve how to feed in list of fieldnames from CLI
- add out_file name as an option for CLI args to specify
- run more performance and efficiency tests (@profile, memory_profiler etc)
- run more tests with different sized/structured json inputs
