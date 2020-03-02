#!/usr/bin/env python3

# The MIT License (MIT)
# =====================
#
# Copyright © 2020 Azavea
#
# Permission is hereby granted, free of charge, to any person
# obtaining a copy of this software and associated documentation
# files (the “Software”), to deal in the Software without
# restriction, including without limitation the rights to use,
# copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the
# Software is furnished to do so, subject to the following
# conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED “AS IS”, WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
# OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
# NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
# HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
# WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
# FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.

import argparse
import ast
import json
import os


def cli_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument('--architecture', required=False, type=str)
    parser.add_argument('--bounds-clip', required=False,
                        default=True, type=ast.literal_eval)
    parser.add_argument('--dryrun', required=False,
                        default=False, type=ast.literal_eval)
    parser.add_argument('--localrun', required=False,
                        default=False, type=ast.literal_eval)
    parser.add_argument('--gather', required=True, type=str)
    parser.add_argument('--jobdef', required=False, type=str)
    parser.add_argument('--jobqueue', required=False, type=str)
    parser.add_argument('--name', required=True, type=str)
    parser.add_argument('--output-path', required=True, type=str)
    parser.add_argument('--response', required=True, type=str)
    parser.add_argument('--weights', required=False, type=str)
    return parser


if __name__ == '__main__':
    args = cli_parser().parse_args()

    with open(args.response, 'r') as f:
        response = json.load(f)
    [xmin, ymin, xmax, ymax] = response.get('bounds')
    results = response.get('selections')

    idxs = range(1, len(results)+1)
    for (i, result) in zip(idxs, results):
        gather_cmd = [
            args.gather,
            '--name', args.name,
            '--index', i,
            '--output-path', args.output_path,
            '--sentinel-path', result.get('sceneMetadata').get('path'),
            '--backstop', result.get('backstop', False)
        ]

        if args.bounds_clip:
            gather_cmd.extend(['--bounds', xmin, ymin, xmax, ymax])
        if args.architecture:
            gather_cmd.extend(['--architecture', args.architecture])
        if args.weights:
            gather_cmd.extend(['--weights', args.weights])

        gather_cmd = [str(x) for x in gather_cmd]

        if args.localrun:
            submission = 'python ' + ' '.join(gather_cmd)
        else:
            if not (args.jobqueue and args.jobdef):
                raise ValueError('Must supply jobqueue and jobdef if not localrun')
            submission = ''.join([
                'aws batch submit-job ',
                '--job-name {} '.format('{}-{}'.format(args.name, i)),
                '--job-queue {} '.format(args.jobqueue),
                '--job-definition {} '.format(args.jobdef),
                '--container-overrides vcpus=2,memory=15000,',
                'command=./download_run.sh,' + ','.join(gather_cmd)])

        if args.dryrun:
            print(submission)
        else:
            os.system(submission)
