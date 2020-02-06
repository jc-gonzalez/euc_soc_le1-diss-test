#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
LE1 Dissemination Test Execution

Generates a set of
Generates
"""
#----------------------------------------------------------------------

import os
import sys
import argparse

from pprint import pprint
from datetime import datetime, timedelta

from ingest_client_sc456 import main as ab_ingest_fn
from le1_metadata_creator import LE1_VIS_Metadata_Creator

from le1_disstest_run import LE1_Disseminator, EndOfIngestion

#----------------------------------------------------------------------

_filedir_ = os.path.dirname(os.path.realpath(__file__))
_appsdir_, _ = os.path.split(_filedir_)
_basedir_, _ = os.path.split(_appsdir_)
sys.path.insert(0, os.path.abspath(os.path.join(_filedir_, _basedir_, _appsdir_)))

PYTHON2 = False
PY_NAME = "python3"

import logging
logger = logging.getLogger()

#----------------------------------------------------------------------

VERSION = '0.0.1'

__author__     = "J. C. Gonzalez"
__version__    = VERSION
__license__    = "LGPL 3.0"
__status__     = "Development"
__copyright__  = "Copyright (C) 2015-2020 by Euclid SOC Team @ ESAC / ESA"
__email__      = "jcgonzalez@sciops.esa.int"
__date__       = "2020-02-03"
__maintainer__ = "Euclid SOC Team"
#__url__       = ""

#----------------------------------------------------------------------

def getArgs():
    """
    Parse arguments from command line
    :return: args structure
    """
    parser = argparse.ArgumentParser(description='Script to launch the ingestion of LE1 products' +
                                                 ' into the DSS for the Dissemination tests',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('-n', '--num_obs', dest='num_obs', type=int, default=20,
                        help='Number of observations to ingest per execution')
    parser.add_argument('-s', '--sleep', dest='sleep', type=int, default=60,
                        help='Number of seconds to wait between file ingestions')

    return parser.parse_args()

def main():
    """
    Process LE1 dissemination generated JSON file, creates the appropriate LE1 product
    data and metadata files, and ingest them in the EAS.
    """
    args = getArgs()

    le1diss = LE1_Disseminator(num_obs=args.num_obs, sleep=args.sleep)

    try:
        le1diss.run()
    except EndOfIngestion as ei:
        print('Exiting: {}'.format(ei))
    except Exception as ee:
        print('Error: {}'.format(ee))
        print(ee.with_traceback())


if __name__ == '__main__':
    main()
