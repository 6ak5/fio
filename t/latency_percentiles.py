#!/usr/bin/env python3
# SPDX-License-Identifier: GPL-2.0-only
#
# Copyright (c) 2020 Western Digital Corporation or its affiliates.
#
"""
# latency-percentiles.py
#
# Test the code that produces latency percentiles
# This is mostly to test the code changes to allow reporting
# of slat, clat, and lat percentiles
#
# USAGE
# python3 latency-tests.py [-f fio-path] [-a artifact-root]
#
# Test the ?lat_percentiles options:
#
# - DONE terse
#   produce both terse, JSON output and confirm that they match
#   lat only; clat only; both lat and clat
# - normal output: manually test
#       null ioengine
#           enable all, but only clat and lat appear
#           enable subset of latency types
#           read, write, trim, unified
#       libaio ioengine
#           enable all latency types
#           enable subset of latency types
#           read, write, trim, unified
# - DONE sync_lat
#   confirm that sync_lat data appears
# - DONE json
#   unified rw reporting
#   compare with latency log
#   try various combinations of the ?lat_percentile options
#   null, aio
#   r, w, t
# - DONE json+
#   check presence of latency bins
#   if the json percentiles match those from the raw data
#   then the latency bin values and counts are probably ok
"""

import os
import csv
import sys
import json
import math
import time
import argparse
import platform
import subprocess
from pathlib import Path


def run_fio(fio, artifact_root, test):
    """Run a fio test.

    fio             fio executable location
    artifact_root   root directory for artifacts (subdirectory will be created under here)
    test            test specification
    """

    test_dir = os.path.join(artifact_root, "{:03d}".format(test['test_id']))
    if not os.path.exists(test_dir):
        os.mkdir(test_dir)

    filename = "latency{:03d}".format(test['test_id'])

    fio_args = [
        "--name=latency",
        "--randrepeat=0",
        "--norandommap",
        "--time_based",
        "--size=512M",
        "--rwmixread=50",
        "--write_lat_log={0}".format(filename),
        "--output={0}.out".format(filename),
        "--ioengine={ioengine}".format(**test),
        "--rw={rw}".format(**test),
        "--runtime={runtime}".format(**test),
        "--output-format={output-format}".format(**test),
    ]
    for opt in ['slat_percentiles', 'clat_percentiles', 'lat_percentiles',
                'unified_rw_reporting', 'fsync', 'fdatasync']:
        if opt in test:
            option = '--{0}={{{0}}}'.format(opt)
            fio_args.append(option.format(**test))

    command = [fio] + fio_args
    command_file = open(os.path.join(test_dir, "{0}.command".format(filename)), "w+")
    command_file.write("%s\n" % command)
    command_file.close()

    passed = True
    stdout_file = open(os.path.join(test_dir, "{0}.stdout".format(filename)), "w+")
    stderr_file = open(os.path.join(test_dir, "{0}.stderr".format(filename)), "w+")
    exitcode_file = open(os.path.join(test_dir, "{0}.exitcode".format(filename)), "w+")
    try:
        proc = None
        # Avoid using subprocess.run() here because when a timeout occurs,
        # fio will be stopped with SIGKILL. This does not give fio a
        # chance to clean up and means that child processes may continue
        # running and submitting IO.
        proc = subprocess.Popen(command,
                                stdout=stdout_file,
                                stderr=stderr_file,
                                cwd=test_dir,
                                universal_newlines=True)
        proc.communicate(timeout=300)
        exitcode_file.write('{0}\n'.format(proc.returncode))
        passed &= (proc.returncode == 0)
    except subprocess.TimeoutExpired:
        proc.terminate()
        proc.communicate()
        assert proc.poll()
        print("Timeout expired")
        passed = False
    except Exception:
        if proc:
            if not proc.poll():
                proc.terminate()
                proc.communicate()
        print("Exception: %s" % sys.exc_info())
        passed = False
    finally:
        stdout_file.close()
        stderr_file.close()
        exitcode_file.close()

    return passed, test_dir, filename


def get_json(filename):
    """Convert fio JSON output into a python JSON object

    filename        file containing fio JSON output
    """

    with open(filename, 'r') as file:
        file_data = file.read()

    #
    # Sometimes fio informational messages are included at the top of the
    # JSON output, especially under Windows. Try to decode output as JSON
    # data, lopping off up to the first four lines
    #
    lines = file_data.splitlines()
    for i in range(5):
        file_data = '\n'.join(lines[i:])
        try:
            json_data = json.loads(file_data)
        except json.JSONDecodeError:
            continue
        else:
            return True, json_data

    return False, ''


def get_terse(filename):
    """Read fio output and return terse format data.

    filename        file containing fio output
    """

    with open(filename, 'r') as file:
        file_data = file.read()

    #
    # Read the first few lines and see if any of them begin with '3;fio-'
    # If so, the line is probably terse output. Obviously, this only
    # works for fio terse version 3 and it does not work for
    # multi-line terse output
    #
    lines = file_data.splitlines()
    for i in range(8):
        file_data = lines[i]
        if file_data.startswith('3;fio-'):
            return True, file_data.split(';')

    return False, ''


#
# ddir is for fio latency logs
#   0   read
#   1   write
#   2   trim
#
def check_latencies(test_dir, filename, jsondata, ddir, slat=True, clat=True, tlat=True, plus=False,
                    unified=False):
    """Check fio latency data.

    test_dir            directory containing test artifacts
    filename            filename stub for test data
    jsondata            fio JSON output
    ddir                data direction to check (0=read, 1=write, 2=trim)
    slat                True if submission latency data available to check
    clat                True if completion latency data available to check
    tlat                True of total latency data available to check
    plus                True if we actually have json+ format data where additional checks can
                        be carried out
    unified             True if fio is reporting unified r/w data
    """

    types = {
        'slat': slat,
        'clat': clat,
        'lat': tlat
    }

    retval = True

    for lat in ['slat', 'clat', 'lat']:
        this_iter = True
        if not types[lat]:
            if 'percentile' in jsondata[lat+'_ns']:
                this_iter = False
                print('unexpected %s percentiles found' % lat)
            else:
                print("%s percentiles skipped" % lat)
            continue
        else:
            if 'percentile' not in jsondata[lat+'_ns']:
                this_iter = False
                print('%s percentiles not found in fio output' % lat)

#
# Check only for the presence/absence of json+
# latency bins. Future work can check the
# accurracy of the bin values and counts.
#
# Because the latency percentiles are based on
# the bins, we can be confident that the bin
# values and counts are correct if fio's
# latency percentiles match what we compute
# from the raw data.
#
        if plus:
            if 'bins' not in jsondata[lat+'_ns']:
                print('bins not found with json+ output format')
                this_iter = False
            else:
                if not check_jsonplus(jsondata[lat+'_ns']):
                    this_iter = False
        else:
            if 'bins' in jsondata[lat+'_ns']:
                print('json+ bins found with json output format')
                this_iter = False

        lat_file = os.path.join(test_dir, "{0}_{1}.1.log".format(filename, lat))
        latencies = []
        with open(lat_file, 'r', newline='') as file:
            reader = csv.reader(file)
            for line in reader:
                if unified or int(line[2]) == ddir:
                    latencies.append(int(line[1]))

        if int(jsondata['total_ios']) != len(latencies):
            this_iter = False
            print('%s: total_ios = %s, latencies logged = %d' % \
                    (lat, jsondata['total_ios'], len(latencies)))

        latencies.sort()
        ptiles = jsondata[lat+'_ns']['percentile']

        for percentile in ptiles.keys():
            #
            # numpy.percentile(latencies, float(percentile),
            #       interpolation='higher')
            # produces values that mostly match what fio reports
            # however, in the tails of the distribution, the values produced
            # by fio's and numpy.percentile's algorithms are occasionally off
            # by one latency measurement. So instead of relying on the canned
            # numpy.percentile routine, implement here fio's algorithm
            #
            rank = math.ceil(float(percentile)/100 * len(latencies))
            if rank > 0:
                index = rank - 1
            else:
                index = 0
            value = latencies[int(index)]
            fio_val = int(ptiles[percentile])
            # The theory in stat.h says that the proportional error will be
            # less than 1/128
            delta = abs(fio_val - value) / value
            if not similar(fio_val, value):
                print("Error with %s %sth percentile: "
                      "fio: %d, expected: %d, proportional delta: %f" %
                      (lat, percentile, fio_val, value, delta))
                print("Rank: %d, index: %d" % (rank, index))
                this_iter = False

        if this_iter:
            print("%s percentiles match" % lat)
        else:
            retval = False

    return retval


def check_empty(job):
    """
    Make sure JSON data is empty.

    Some data structures should be empty. This function makes sure that they are.

    job         JSON object that we need to check for emptiness
    """

    return job['total_ios'] == 0 and \
            job['slat_ns']['N'] == 0 and \
            job['clat_ns']['N'] == 0 and \
            job['lat_ns']['N'] == 0


def similar(estimate, actual):
    """
    Check whether the estimated values recorded by fio are within the theoretical bound.

    Since it is impractical to store exact latency measurements for each and every IO, fio
    groups similar latency measurements into variable-sized bins. The theory in stat.h says
    that the proportional error will be less than 1/128. This function checks whether this
    is true.

    estimate        value of the bin used by fio to store a given latency
    actual          actual latency value
    """
    delta = abs(estimate - actual) / actual
    return delta <= 1/128


#
# Simple json+ bin consistency checks
#
# check only the min, max, and size
#
def check_jsonplus(jsondata):
    """Check consistency of json+ data

    When we have json+ data we can check the min value, max value, and sample size reported by fio

    jsondata            json+ data that we need to check
    """

    retval = True

    keys = [int(k) for k in jsondata['bins'].keys()]
    values = [int(jsondata['bins'][k]) for k in jsondata['bins'].keys()]
    smallest = min(keys)
    biggest = max(keys)
    sampsize = sum(values)

    if not similar(jsondata['min'], smallest):
        retval = False
        print('reported min %d does not match json+ min %d' % (jsondata['min'], smallest))

    if not similar(jsondata['max'], biggest):
        retval = False
        print('reported max %d does not match json+ max %d' % (jsondata['max'], biggest))

    if sampsize != jsondata['N']:
        retval = False
        print('reported sample size %d does not match json+ total count %d' % \
                (jsondata['N'], sampsize))

    return retval


def check_sync_lat(jsondata, plus=False):
    """Check fsync latency percentile data.

    All we can check is that some percentiles are reported, unless we have json+ data.
    If we actually have json+ data then we can do more checking.

    jsondata        JSON data for fsync operations
    plus            True if we actually have json+ data
    """
    retval = True

    if 'percentile' not in jsondata['lat_ns']:
        print("Sync percentile data not found")
        return False

    if int(jsondata['total_ios']) != int(jsondata['lat_ns']['N']):
        retval = False
        print('Mismatch between total_ios and lat_ns sample size')

    if not plus:
        if 'bins' in jsondata['lat_ns']:
            print('Unexpected json+ bin data found')
            return False

    if not check_jsonplus(jsondata['lat_ns']):
        retval = False

    return retval


def check_terse(terse, jsondata):
    """Compare terse latencies with JSON latencies.

    terse           terse format data for checking
    jsondata        JSON format data for checking
    """

    retval = True

    for lat in terse:
        split = lat.split('%')
        pct = split[0]
        terse_val = int(split[1][1:])
        json_val = math.floor(jsondata[pct]/1000)
        if terse_val != json_val:
            retval = False
            print('Mismatch with %sth percentile: json value=%d,%d terse value=%d' % \
                    (pct, jsondata[pct], json_val, terse_val))

    return retval


def check_t001(test_dir, filename):
    """Check Test 1 output.

    test_dir        artifact directory
    filename        filename stub for output files
    """

    output_file = os.path.join(test_dir, "{0}.out".format(filename))
    status, json_data = get_json(output_file)
    if not status:
        return False, 'could not decode JSON data'

    job = json_data['jobs'][0]

    retval = True
    if not check_empty(job['write']):
        print("Unexpected write data found in output")
        retval = False
    if not check_empty(job['trim']):
        print("Unexpected trim data found in output")
        retval = False

    retval &= check_latencies(test_dir, filename, job['read'], 0, slat=False)

    return retval


def check_t002(test_dir, filename):
    """Check Test 2 output.

    test_dir        artifact directory
    filename        filename stub for output files
    """
    output_file = os.path.join(test_dir, "{0}.out".format(filename))
    status, json_data = get_json(output_file)
    if not status:
        return False, 'could not decode JSON data'

    job = json_data['jobs'][0]

    retval = True
    if not check_empty(job['read']):
        print("Unexpected read data found in output")
        retval = False
    if not check_empty(job['trim']):
        print("Unexpected trim data found in output")
        retval = False

    retval &= check_latencies(test_dir, filename, job['write'], 1, slat=False, clat=False)

    return retval


def check_t003(test_dir, filename):
    """Check Test 3 output.

    test_dir        artifact directory
    filename        filename stub for output files
    """

    output_file = os.path.join(test_dir, "{0}.out".format(filename))
    status, json_data = get_json(output_file)
    if not status:
        return False, 'could not decode JSON data'

    job = json_data['jobs'][0]

    retval = True
    if not check_empty(job['read']):
        print("Unexpected read data found in output")
        retval = False
    if not check_empty(job['write']):
        print("Unexpected write data found in output")
        retval = False

    retval &= check_latencies(test_dir, filename, job['trim'], 2, slat=False, tlat=False)

    return retval


def check_t004(test_dir, filename):
    """Check Test 4 output.

    test_dir        artifact directory
    filename        filename stub for output files
    """

    output_file = os.path.join(test_dir, "{0}.out".format(filename))
    status, json_data = get_json(output_file)
    if not status:
        return False, 'could not decode JSON data'

    job = json_data['jobs'][0]

    retval = True
    if not check_empty(job['write']):
        print("Unexpected write data found in output")
        retval = False
    if not check_empty(job['trim']):
        print("Unexpected trim data found in output")
        retval = False

    retval &= check_latencies(test_dir, filename, job['read'], 0, plus=True)

    return retval


def check_t005(test_dir, filename):
    """Check Test 5 output.

    test_dir        artifact directory
    filename        filename stub for output files
    """

    output_file = os.path.join(test_dir, "{0}.out".format(filename))
    status, json_data = get_json(output_file)
    if not status:
        return False, 'could not decode JSON data'

    job = json_data['jobs'][0]

    retval = True
    if not check_empty(job['read']):
        print("Unexpected read data found in output")
        retval = False
    if not check_empty(job['trim']):
        print("Unexpected trim data found in output")
        retval = False

    retval &= check_latencies(test_dir, filename, job['write'], 1, slat=False, plus=True)

    return retval


def check_t006(test_dir, filename):
    """Check Test 6 output.

    test_dir        artifact directory
    filename        filename stub for output files
    """

    output_file = os.path.join(test_dir, "{0}.out".format(filename))
    status, json_data = get_json(output_file)
    if not status:
        return False, 'could not decode JSON data'

    job = json_data['jobs'][0]

    retval = True
    if not check_empty(job['write']):
        print("Unexpected write data found in output")
        retval = False
    if not check_empty(job['trim']):
        print("Unexpected trim data found in output")
        retval = False

    retval &= check_latencies(test_dir, filename, job['read'], 0, slat=False, tlat=False, plus=True)

    return retval


def check_t007(test_dir, filename):
    """Check Test 7 output.

    test_dir        artifact directory
    filename        filename stub for output files
    """

    output_file = os.path.join(test_dir, "{0}.out".format(filename))
    status, json_data = get_json(output_file)
    if not status:
        return False, 'could not decode JSON data'

    job = json_data['jobs'][0]

    retval = True
    if not check_empty(job['trim']):
        print("Unexpected trim data found in output")
        retval = False

    retval &= check_latencies(test_dir, filename, job['read'], 0, \
            slat=True, clat=False, tlat=False, plus=True)
    retval &= check_latencies(test_dir, filename, job['write'], 1, \
            slat=True, clat=False, tlat=False, plus=True)

    return retval


def check_t008(test_dir, filename):
    """Check Test 8 output.

    test_dir        artifact directory
    filename        filename stub for output files
    """

    output_file = os.path.join(test_dir, "{0}.out".format(filename))
    status, json_data = get_json(output_file)
    if not status:
        return False, 'could not decode JSON data'

    job = json_data['jobs'][0]

    retval = True
    if 'read' in job or 'write'in job or 'trim' in job:
        print("Unexpected data direction found in fio output")
        retval = False

    retval &= check_latencies(test_dir, filename, job['mixed'], 0, plus=True, unified=True)

    return retval


def check_t009(test_dir, filename):
    """Check Test 9 output.

    test_dir        artifact directory
    filename        filename stub for output files
    """

    output_file = os.path.join(test_dir, "{0}.out".format(filename))
    status, json_data = get_json(output_file)
    if not status:
        return False, 'could not decode JSON data'

    job = json_data['jobs'][0]

    retval = True
    if not check_empty(job['read']):
        print("Unexpected read data found in output")
        retval = False
    if not check_empty(job['trim']):
        print("Unexpected trim data found in output")
        retval = False
    if not check_sync_lat(job['sync'], plus=True):
        print("Error checking fsync latency data")
        retval = False

    retval &= check_latencies(test_dir, filename, job['write'], 1, slat=False, plus=True)

    return retval


def check_t010(test_dir, filename):
    """Check Test 10 output.

    test_dir        artifact directory
    filename        filename stub for output files
    """

    output_file = os.path.join(test_dir, "{0}.out".format(filename))
    status, json_data = get_json(output_file)
    if not status:
        return False, 'could not decode JSON data'

    job = json_data['jobs'][0]

    status, terse = get_terse(output_file)
    if not status:
        return False, 'could not decode terse data'

    retval = True
    if not check_empty(job['trim']):
        print("Unexpected trim data found in output")
        retval = False

    retval &= check_latencies(test_dir, filename, job['read'], 0, plus=True)
    retval &= check_latencies(test_dir, filename, job['write'], 1, plus=True)
    retval &= check_terse(terse[17:34], job['read']['clat_ns']['percentile'])
    retval &= check_terse(terse[58:75], job['write']['clat_ns']['percentile'])
    # Terse data checking only works for default percentiles.
    # This needs to be changed if something other than the default is ever used.

    return retval


def check_t011(test_dir, filename):
    """Check Test 11 output.

    test_dir        artifact directory
    filename        filename stub for output files
    """

    output_file = os.path.join(test_dir, "{0}.out".format(filename))
    status, json_data = get_json(output_file)
    if not status:
        return False, 'could not decode JSON data'

    job = json_data['jobs'][0]

    status, terse = get_terse(output_file)
    if not status:
        return False, 'could not decode terse data'

    retval = True
    if not check_empty(job['trim']):
        print("Unexpected trim data found in output")
        retval = False

    retval &= check_latencies(test_dir, filename, job['read'], 0, \
            slat=False, clat=False, plus=True)
    retval &= check_latencies(test_dir, filename, job['write'], 1, \
            slat=False, clat=False, plus=True)
    retval &= check_terse(terse[17:34], job['read']['lat_ns']['percentile'])
    retval &= check_terse(terse[58:75], job['write']['lat_ns']['percentile'])
    # Terse data checking only works for default percentiles.
    # This needs to be changed if something other than the default is ever used.

    return retval


def parse_args():
    """Parse command-line arguments."""

    parser = argparse.ArgumentParser()
    parser.add_argument('-f', '--fio', help='path to file executable (e.g., ./fio)')
    parser.add_argument('-a', '--artifact-root', help='artifact root directory')
    args = parser.parse_args()

    return args


def main():
    """Run tests of fio latency percentile reporting"""

    args = parse_args()

    artifact_root = args.artifact_root if args.artifact_root else \
        "latency-test-{0}".format(time.strftime("%Y%m%d-%H%M%S"))
    os.mkdir(artifact_root)
    print("Artifact directory is %s" % artifact_root)

    if args.fio:
        fio = str(Path(args.fio).absolute())
    else:
        fio = 'fio'
    print("fio path is %s" % fio)

    if platform.system() == 'Linux':
        aio = 'libaio'
    elif platform.system() == 'Windows':
        aio = 'windowsaio'
    else:
        aio = 'posixaio'

    tests = [
        {
            # randread, null
            # enable slat, clat, lat
            # only clat and lat will appear because
            # because the null ioengine is syncrhonous
            "test_id": 1,
            "runtime": 2,
            "output-format": "json",
            "slat_percentiles": 1,
            "clat_percentiles": 1,
            "lat_percentiles": 1,
            "ioengine": 'null',
            'rw': 'randread',
            "check": check_t001,
        },
        {
            # randwrite, null
            # enable lat only
            "test_id": 2,
            "runtime": 2,
            "output-format": "json",
            "slat_percentiles": 0,
            "clat_percentiles": 0,
            "lat_percentiles": 1,
            "ioengine": 'null',
            'rw': 'randwrite',
            "check": check_t002,
        },
        {
            # randtrim, null
            # enable clat only
            "test_id": 3,
            "runtime": 2,
            "output-format": "json",
            "slat_percentiles": 0,
            "clat_percentiles": 1,
            "lat_percentiles": 0,
            "ioengine": 'null',
            'rw': 'randtrim',
            "check": check_t003,
        },
        {
            # randread, aio
            # enable slat, clat, lat
            # all will appear because liaio is asynchronous
            "test_id": 4,
            "runtime": 5,
            "output-format": "json+",
            "slat_percentiles": 1,
            "clat_percentiles": 1,
            "lat_percentiles": 1,
            "ioengine": aio,
            'rw': 'randread',
            "check": check_t004,
        },
        {
            # randwrite, aio
            # enable only clat, lat
            "test_id": 5,
            "runtime": 5,
            "output-format": "json+",
            "slat_percentiles": 0,
            "clat_percentiles": 1,
            "lat_percentiles": 1,
            "ioengine": aio,
            'rw': 'randwrite',
            "check": check_t005,
        },
        {
            # randread, aio
            # by default only clat should appear
            "test_id": 6,
            "runtime": 5,
            "output-format": "json+",
            "ioengine": aio,
            'rw': 'randread',
            "check": check_t006,
        },
        {
            # 50/50 r/w, aio
            # enable only slat
            "test_id": 7,
            "runtime": 5,
            "output-format": "json+",
            "slat_percentiles": 1,
            "clat_percentiles": 0,
            "lat_percentiles": 0,
            "ioengine": aio,
            'rw': 'randrw',
            "check": check_t007,
        },
        {
            # 50/50 r/w, aio, unified_rw_reporting
            # enable only slat, clat, lat
            "test_id": 8,
            "runtime": 5,
            "output-format": "json+",
            "slat_percentiles": 1,
            "clat_percentiles": 1,
            "lat_percentiles": 1,
            "ioengine": aio,
            'rw': 'randrw',
            'unified_rw_reporting': 1,
            "check": check_t008,
        },
        {
            # randwrite, null
            # enable slat, clat, lat
            # fsync
            "test_id": 9,
            "runtime": 2,
            "output-format": "json+",
            "slat_percentiles": 1,
            "clat_percentiles": 1,
            "lat_percentiles": 1,
            "ioengine": 'null',
            'rw': 'randwrite',
            'fsync': 32,
            "check": check_t009,
        },
        {
            # 50/50 r/w, aio
            # enable slat, clat, lat
            "test_id": 10,
            "runtime": 5,
            "output-format": "terse,json+",
            "slat_percentiles": 1,
            "clat_percentiles": 1,
            "lat_percentiles": 1,
            "ioengine": aio,
            'rw': 'randrw',
            "check": check_t010,
        },
        {
            # 50/50 r/w, aio
            # enable only lat
            "test_id": 11,
            "runtime": 5,
            "output-format": "terse,json+",
            "slat_percentiles": 0,
            "clat_percentiles": 0,
            "lat_percentiles": 1,
            "ioengine": aio,
            'rw': 'randrw',
            "check": check_t011,
        },
    ]

    passed = 0
    failed = 0

    for test in tests:
        status, test_dir, filename = run_fio(fio, artifact_root, test)
        if status:
            status = test['check'](test_dir, filename)
        outcome = 'PASSED' if status else 'FAILED'
        print("Test {0} {1}".format(test['test_id'], outcome))
        if status:
            passed = passed + 1
        else:
            failed = failed + 1

    print("{0} tests passed, {1} failed".format(passed, failed))

    sys.exit(failed)


if __name__ == '__main__':
    main()
