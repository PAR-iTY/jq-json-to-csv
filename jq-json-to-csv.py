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


def config(input='', columns='', uid=''):

    if not input:
        input = 'json/test.json'

    # list of column names
    # expect from CLI: comma seperated string wrapped in quotes

    if not columns:
        if input == 'json/test.json':
            # (these are just for example to match supplied test.json file)
            columns = ['balance', 'eyeColor', 'company', 'name']

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
#               FUNCTIONS                 #
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
        yield line

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
        # deserialise line string
        line_obj = json.loads(line)

        # skip falsy values
        if not line_obj[1]:
            continue

        # loop through target columns
        for col in columns:

            # if target is the last path item
            if col == line_obj[0][-1]:

                # save path length if it doesnt exist yet
                if not row_paths[col]:
                    row_paths[col] = line_obj[0]

                # detect overwrite if row already has a col value
                # if it does, need to enumerate to col.n
                if row[col]:

                    # if col == row uid then start a new row instead
                    if col == uid and len(row_paths[col]) == len(line_obj[0]):
                        # print(col, len(row_paths[col]), len(line_obj[0]))
                        # table id column name matches latest path piece

                        # iteration has hit second uid col
                        # yield finished old row
                        yield row

                        # reset row dicts
                        # (no longer with inherited keys from previous row)
                        row = copy(row_base)
                        row_paths = copy(row_base)

                        # add new row uid to new row dicts
                        row[uid] = line_obj[1]
                        row_paths[uid] = line_obj[0]
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

                    row[f'{col}.{count}'] = line_obj[1]
                    break

                # if code-execution reaches here:
                # must be a regular col to add
                row[col] = line_obj[1]
                row_paths[col] = line_obj[0]

                break

        # end of column loop

    # catch final row (will at least have a new populated uid)
    yield row


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
