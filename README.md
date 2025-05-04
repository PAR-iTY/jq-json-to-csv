> work in progress personal project

# JSON parser

Generic JSON parser designed to handle edge cases that commonly block an automation pathway. JSON is un or semi structured and often schema-less and/or arbitrarily nested. JSON files may also be too large to parse in one go, and therefore must be streamed. Streaming then compounds the problems with structuring because structure must now be predicted and understood line-by-line as the stream arrives. Using Python and jq the unknowns of structure and size can be solved together.  

- uses jq and python to process JSON data with undefined structure and nesting
- accepts target fieldnames and JSON input, processes line by line and outputs CSV
- enumerates duplicate fieldname values found in nested JSON objects
- uses 2-pass parsing to get all possible fieldnames in first pass
- handles unpredictable nesting and number of duplicate target fieldnames
- accepts target columns from CLI args, delimited by ", " (comma + space)

## requirements

- jq command line parser: https://github.com/stedolan/jq
- Python Fire CLI arguments generator: https://github.com/google/python-fire

## components

### jq JSON parser

- Can: pretty-print, filter, reformat, flatten, stream, etc
- Handles different JSON formats (whitespace and newline etc) well
- Handles n-nested structures (either lists or dictionaries) well
- Streaming creates a reliable structured intermediary form: [<path>, <leaf-value>]
- Can output stream back into JSON form, as array of values or flattened dictionary etc

### Python

- Good JSON handling environment to then do something else with the data
- Can add streaming functionality for handling huge JSON volumes
- Can use subprocesses to communicate with shell tools
- Can pipe to jq and get piped output back for large json
- Can either parameterise jq to handle JSON, or generically parse with jq and pick apart and assemble line by line with Python
- Similar Python with well known libraries could handle XML blobs

### jq.py script

- This Python script is for handling arbitrarily structured and sized JSON input
demonstrates python can call jq to stream JSON, and accept jq's stdout as a stream too
- jq command streams and flattens JSON into a { "path.to.key": "value" } single line object
- Could be achieved with a Docker jq image (Debian OS) → expect Python to be included
- csv.writer and csv.DictWriter transform lists and dicts respectively into CSV data
- "The % operator works conveniently to substitute values from a dict into a string by name"

```
split processing:  
1) fieldnames=COLUMNS  
2) fieldnames=data_cols  

non-direct path matches, path recurse approaches must use fieldnames=data_cols  

row[id] approach needs to be smarter about multiple column name matches dont want to overwrite

- this demonstrates python can command jq to stream JSON in different formats
- python accepts jq's standard out as a line-by-line stream for processing
- jq and python can share processing work based on their strengths
- currently jq determines the inclusion and format of the line, and the path format
- python is (if needed) in charge of re-creating objects and representing nesting

how should this be used? useful features are:
path-as-list length, the desired value, it's immediate parent
and if the parent is a list, the parents parent too (ie we want a key)

what if jq turned all lists into dicts? pointless?
im kinda thinking the path-as-list is a very helpful thing

user params: some json input, a list of column names OR description of rows
a set of functions needs to handle some different filter/search requests:

1) whether streaming is required  
2) whether flattening is required  
3) whether delim care is required  

    until row[ID] is given a value for the second time, keep adding to row
    if row has a populated id
    and
    table ID matches incoming key (new id may be incoming)
    and
    row id does not equal incoming id
    if row[ID] and ID == obj[0][-1] and row[ID] != obj[1]:

writerow takes 1-dimensional data (one row)
writerows takes 2-dimensional data (multiple rows)

dictWriter.writerows expects a list of dicts
writer.writerows expects a list of lists

how to get keys from constructed flat data_dict:
gets keys as list: list(data)
iterable alternative: [*data]

open with newline="" to avoid writer blank row inserts
csv.writer with quoting=csv.QUOTE_ALL wraps all cells in ""

open csv, start writer, add columns (use COLUMNS or data_dict keys)
```

## todo

- standardise terminology: 'fieldname'/'column', and 'row'/'line' etc
- re-think: can 2-pass strategy be bypassed without bottlenecking memory?
- re-think/improve row and row_path state management and object copying
- fieldnames pass could use a slimmer gen_process_lines() function
- improve how to feed in list of fieldnames from CLI
- add out_file name as an option for CLI args to specify
- run more performance and efficiency tests (@profile, memory_profiler etc)
- run more tests with different sized/structured json inputs
