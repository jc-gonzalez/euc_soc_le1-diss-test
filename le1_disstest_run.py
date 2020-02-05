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
import json

from pprint import pprint
from datetime import datetime, timedelta

from ingest_client_sc456 import main as ab_ingest_fn
from le1_metadata_creator import \
    LE1_VIS_Metadata_Creator, \
    LE1_NIR_Metadata_Creator, \
    LE1_SIR_Metadata_Creator

#----------------------------------------------------------------------

_filedir_ = os.path.dirname(os.path.realpath(__file__))
_appsdir_, _ = os.path.split(_filedir_)
_basedir_, _ = os.path.split(_appsdir_)
sys.path.insert(0, os.path.abspath(os.path.join(_filedir_, _basedir_, _appsdir_)))

PYTHON2 = False
PY_NAME = "python3"

import logging

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

class LE1_Disseminator:
    """
    Class LE1_Disseminator

    Class to execute the generation and ingestion of LE1 products
    """
    def __init__(self):
        self.logger = logging.getLogger()
        self.le1_vis_meta = LE1_VIS_Metadata_Creator(self.logger)
        self.le1_nir_meta = LE1_NIR_Metadata_Creator(self.logger)
        self.le1_sir_meta = LE1_SIR_Metadata_Creator(self.logger)
        self.obsidfile = os.path.join(_filedir_, 'obsid.dat')
        self.le1_schedules_activities = os.path.join(_filedir_, 'data/le1_generated.json')

    def prepare(self):
        """
        Prepare state, according to latest saves state
        """
        # Check if obsid.dat file exists, with the last obsid processed (and related info)
        if os.path.isfile(self.obsidfile):
            with open(self.obsidfile,'r') as fobs:
                obsid_data = json.load(fobs)
            # Prepare logging system
            last_exec = obsid_data["last_execution"]
            self.configureLogs(logging.INFO, os.path.join(_filedir_, f'log/le1_diss_run_{last_exec + 1}.log'))
            self.logger.info('Resuming dissemination test (ingestion of LE1 data)')
            return (obsid_data["last_obs"],
                   obsid_data["last_row"],
                   last_exec)
        else:
            # Prepare logging system
            self.configureLogs(logging.INFO, os.path.join(_filedir_, 'log/le1_diss_run_1.log'))
            self.logger.info('Starting dissemination test (ingestion of LE1 data)')
            return (0, -1, 0)

    def configureLogs(self, lvl, logfile):
        """
        Function to configure the output of the log system, to be used across the
        entire application.
        :param lvl: Log level for the console log handler
        :return: -
        """
        self.logger.setLevel(logging.DEBUG)

        # Create handlers
        c_handler = logging.StreamHandler()
        f_handler = logging.FileHandler(logfile)
        c_handler.setLevel(lvl)
        f_handler.setLevel(logging.DEBUG)

        # Create formatters and add it to handlers
        c_format = logging.Formatter('%(asctime)s %(levelname).1s %(name)s %(module)s:%(lineno)d %(message)s',
                                     datefmt='%y-%m-%d %H:%M:%S')
        f_format = logging.Formatter('{asctime} {levelname:4s} {name:s} {module:s}:{lineno:d} {message}',
                                     style='{')
        c_handler.setFormatter(c_format)
        f_handler.setFormatter(f_format)

        # Add handlers to the logger
        self.logger.addHandler(c_handler)
        self.logger.addHandler(f_handler)
        lmodules = os.getenv('LOGGING_MODULES', default='').split(':')
        for lname in reversed(lmodules):
            lgr = logging.getLogger(lname)
            if not lgr.handlers:
                lgr.addHandler(c_handler)
                lgr.addHandler(f_handler)
            # logger.debug('Configuring logger with id "{}"'.format(lname))

    def create_product(self, obsid, fldid, seqid, dither, subseq,
                       totseq, t, ra, dec, orient, inst, act, fname):
        """
        Create products for the obs. and epoch
        """
        # Define product id and type
        prodType = fname[:11]
        prodInstance = fname[12:-32]
        prodDate = t
        prodVersion = fname[-10:-5]

        prodId = f'{prodType}-{prodInstance}_{prodVersion}'
        prodFile = fname

        # Create metadata
        sht = 'OPENED'
        cal = 'false'
        chg = 'false'
        if act in ['FF', 'DK', 'BI', 'D']:
            rdout = 'NominalScience'
        elif act == 'CH':
            rdout = 'ChargeInjected'
            sht = 'CLOSED'
            cal = 'true'
            chg = 'true'
        elif act == 'TP':
            rdout = 'PocketPumped'
            sht = 'CLOSED'
            cal = 'true'
            chg = 'false'
        else:
            rdout = 'NominalScience'

        # Set Grism and Filter wheel pos. for NISP
        gwa_pos = ['RGS270', 'RGS000', 'RGS180', 'RGS270']
        if inst in ['SIR', 'NIR']:
            if inst == 'SIR':
                fwa = 'OPENED'
                gwa = gwa_pos[dither - 1]
            else:
                fwa = 'CLOSED' if act == 'D' else act
                gwa = 'OPENED'


        # Set parameter values
        le1meta = self.le1_vis_meta if inst == 'VIS' else \
                 (self.le1_nir_meta if inst == 'NIR' else self.le1_sir_meta)

        le1meta. \
            setProduct(prodId, prodType, prodFile). \
            setSW(). \
            setDates(prodDate). \
            setObsInfo(fldid, obsid, seqid, dither, subseq, totseq). \
            setPointing(ra, dec, orient)

        if inst == 'VIS':
            le1meta.setReadoutAndStatuses(rdout, sht, cal, chg)
        else:
            le1meta.setReadoutAndStatuses(rdout, cal, gwa, fwa)

        # Set folder for the creation of files (and ingestion)
        folder = os.path.join(_filedir_, 'generated')

        # Create metadata file
        xmlfile = os.path.join(folder, fname[:-4] + '.xml')
        with open(xmlfile, 'w') as fxml:
            fxml.write(le1meta.content)

        # Create data file
        datafile = os.path.join(folder, fname)
        with open(datafile, 'w') as fdata:
            fdata.write('')

        return folder


    def ingest_files(self, folder):
        """
        Ingest files into EAS
        """
        Credentials = {"u": 'EC_SOC', "p": 'Eu314_clid'}

        saved_argv = sys.argv
        try:
            sys.argv = ['./ingest_client_sc456.py',
                        'store',
                        folder,
                        '--environment=current',
                        '--project=TEST',
                        '--SDC=SOC',
                        f'--username={Credentials["u"]}',
                        f'--password={Credentials["p"]}']
            #self.logger.info(f'Calling: {sys.argv}')
            #ab_ingest_fn()
        except Exception as ee:
            self.logger.error(str(ee))
        finally:
            sys.argv = saved_argv


    def run(self):
        """
        Process LE1 dissemination generated JSON file, creates the appropriate LE1 product
        data and metadata files, and ingest them in the EAS.
        """

        # Prepare state
        last_obs, last_row, last_exec = self.prepare()

        if last_exec < 0:
            raise Exception('The ingestion of LE1 Products for the LE1 ' +
                            'DSS Dissemination test is completed.')

        # Retrieve data from file
        with open(self.le1_schedules_activities) as fle1:
            le1 = json.load(fle1)

        summ = le1["summary"]

        mint        = summ["mint"]
        maxt        = summ["maxt"]
        minobs      = summ["minobs"]
        maxobs      = summ["maxobs"]
        days        = summ["days"]
        obs_per_day = summ["obs_per_day"]
        npts        = summ["npts"]

        if last_obs < 1:
            final_obs = float(minobs) + obs_per_day - 1
        else:
            final_obs = last_obs + obs_per_day

        self.logger.info(f'====== LE1 Dissemination Test - Execution {last_exec + 1} ======')

        execFinished = False

        num_of_rows = len(le1["obs"])

        row = last_row + 1
        continue_loop = True

        # Loop
        while continue_loop:

            # Get parameters for exposure
            t, dt = le1["time"][row]
            obsid, fldid, seqid, dither, subseq, totseq = le1["obs"][row]
            inst, act = le1["activity"][row]
            ra, dec, orient = le1["point"][row]
            fname, size = le1["file"][row]

            if float(obsid) > final_obs: break

            self.logger.info(f'{obsid}/{final_obs} : Processing {row} : {obsid} ' +
                        f'{t} {dt} {inst} {act} ==> {fname}')

            # Create product (data and metadata)
            fldr = self.create_product(obsid, fldid, seqid, dither, subseq, totseq,
                                  t, ra, dec, orient, inst, act, fname)

            # Ingest files into EAS
            self.ingest_files(folder=fldr)

            # End of loop
            last_obs = float(obsid)
            row = row + 1

            if row >= num_of_rows:
                execFinished = True
                break

        # Ingestion is finished
        # Log the info in the obsid.dat file
        self.logger.info('Storing info in obsid.dat')
        obsid_data = {"last_obs": last_obs,
                      "last_row": row - 1,
                      "last_execution": -1 if execFinished else last_exec + 1}
        with open(self.obsidfile, 'w') as fobs:
            json.dump(obsid_data, fobs)

        self.logger.info(f'Execution {last_exec + 1} completed.')

        if execFinished:
            self.logger.info(f'LE1 DSS Dissemination Test ingestion completed.')


if __name__ == '__main__':
    pass
