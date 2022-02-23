# standard libraries
from copy import copy
import csv
import json
import subprocess
from timeit import default_timer as timer
import os

# non-standard libraries
import fire

# ensures buffers are flushed from child processes
os.environ['PYTHONUNBUFFERED'] = '1'

###########################################
#                  CONFIG                 #
###########################################

# add delimiter back in (something like '__')


def config(input='', columns='', uid=''):

    if not input:
        # input = 'json/test.json'
        input = 'json/jq-test.json'

    # list of column names
    # expect from CLI: comma seperated string wrapped in quotes

    if not columns:
        if input == 'json/test.json':
            # (these are just for example to match supplied test.json file)
            columns = ['balance', 'eyeColor', 'company', 'name']
        elif input == 'json/jq-test.json':
            columns = ['someKey', 'column1', 'column2', 'column3']

    else:
        columns = list(columns.split(', '))

    # nominate first column as the each row's uid
    # cannot ensure a row of data = one entity without knowing when to reset row
    # if an uid cannot be given as input, probably need to flatten all
    if not uid:
        uid = columns[0]

    # standard jq stream output [['path', 'to', 'key'], <leaf-value>]
    cmd = ['jq', '-c', '--stream', '--unbuffered', 'select(length==2)', input]

    return {'columns': columns, 'uid': uid, 'cmd': cmd}

###########################################
#                   JQ                    #
###########################################


def jq_pipe(cmd):
    # setup subprocess to stream:
    # select a jq command
    # use PIPE for standard out
    # wrap standard errors into standard out
    # set buffer size to one line
    # universal_newlines=True
    return subprocess.Popen(
        cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, encoding='utf-8', bufsize=1)


def gen_read_line(jq_out):
    for line in jq_out:

        # is there any gain to doing this minor filtering/processing in this function vs in gen_process_lines?

        # deserialise line string
        line_obj = json.loads(line)

        # skip falsy values
        if not line_obj[1]:
            continue

        yield line_obj

###########################################
#               PROCESSING                #
###########################################


def gen_process_lines(config):

    # vars for convienience
    data = jq_pipe(config['cmd']).stdout
    columns = config['columns']
    uid = config['uid']

    # make base row with target columns and empty values
    row_base = dict.fromkeys(columns)

    # row and row_paths take copies of row_base
    row = copy(row_base)
    row_paths = copy(row_base)

    for line in gen_read_line(data):

        # full-path fieldnames method
        # in this approach, the plain column names are used for detection
        # but all fieldnames will be linePaths, not plain column names
        # this is because the fieldnames are to show the nested structure
        # --> remove the columns from fieldnames after processing

        targetCol = line[0][-1]

        # save path (for length checks)
        linePath = line[0]

        # get path list items as strings and concatenate
        # convert to string first to handle array index ints
        # i.e. some.nested.jq.path.0
        colPath = '.'.join(map(str, linePath))

        if targetCol in columns:

            # detect row uid overwrite
            if targetCol == uid:

                key_match = [key for key in row if key.endswith(
                    targetCol) and len(key.split('.')) == len(linePath)]

                # must run a path-piece length check
                # because colPaths will all be unique due to array indexing

                # if new linePath length is same as one saved in row already

                # key_match expression
                # relies on delimiter not being in key name
                # make delim '__' or something unusual

                # for key in row:
                #     if key.endswith(targetCol) and len(key.split('.')) == len(linePath):
                #         print('key loop match:', key)

                if key_match:
                    print('key_match:', key_match)

                    # iteration has hit second uid col
                    # print('detected new uid')

                    # first yield finished old row
                    yield row

                    # then reset row dict
                    row = copy(row_base)

                    # then add the new uid to the new row
                    # row[colPath] = line[1]

            row[colPath] = line[1]

        print('final row')

        yield row


# enumerate nested fieldname duplicates method
'''
        # loop through target columns
        for col in columns:

            # if target is the last path item
            if col == line[0][-1]:

                # print(line[1])

                # save path length if it doesnt exist yet
                if not row_paths[col]:
                    row_paths[col] = line[0]

                # detect overwrite if row already has a col value
                # if it does, need to enumerate to col.n
                if row[col]:

                    # if col = row uid first yield old row and then start a new row
                    if col == uid and len(row_paths[col]) == len(line[0]):
                        # table id column name matches latest path piece

                        # iteration has hit second uid col
                        # first yield finished old row
                        yield row

                        # then reset row dicts
                        # (no longer with inherited keys from previous row)
                        row = copy(row_base)
                        row_paths = copy(row_base)

                        # add new row uid to new row dicts
                        row[uid] = line[1]
                        row_paths[uid] = line[0]
                        break

                    # if code-execution reaches here:
                    # must be a non-uid duplicate
                    # check how many col-like keys row has
                    # get char-length of column name
                    i = len(col)
                    count = 1

                    # loop through row keys
                    for key in [*row]:
                        if key != col and col in key[:i]:
                            # count matches
                            count += 1

                    row[f'{col}.{count}'] = line[1]
                    break

                # if code-execution reaches here:
                # must be a regular col to add
                row[col] = line[1]
                row_paths[col] = line[0]

                break

        # end of column loop

    # catch final row (will at least have a new populated uid)
    yield row
'''

###########################################
#                   CSV                   #
###########################################


# this function, if necessary, could run a simpler generator function, ie:
# would not need to examine any values in order to get total union_keys set
def get_fieldnames(processed_lines):

    # start with empty set
    union_keys = set()

    for line in processed_lines:
        # union all new fieldnames
        union_keys = union_keys | line.keys()

    # turn set into ordered list
    fieldnames = sorted(list(union_keys))

    return fieldnames


def gen_write_csv(processed_lines, fieldnames):

    # open file in w+ mode truncates any existing data
    with open('csv/jq-out.csv', mode='w+', newline='') as csv_file:
        writer = csv.DictWriter(
            csv_file, fieldnames=fieldnames, quoting=csv.QUOTE_ALL)
        writer.writeheader()
        writer.writerows(processed_lines)


###########################################
#                   RUN                   #
###########################################

if __name__ == '__main__':
    # get data processing configuration
    config = fire.Fire(config)

    # start data processing and csv writing
    start_1 = timer()

    # pass #1: get fieldnames
    fieldnames = get_fieldnames(gen_process_lines(config))

    end_1 = timer()

    print(f'fieldnames completed in {round(end_1 - start_1, 4)} seconds')

    start_2 = timer()

    # pass #2: write values
    gen_write_csv(gen_process_lines(config), fieldnames)

    end_2 = timer()

    print(f'dictwriter completed in {round(end_2 - start_2, 4)} seconds')
