#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
LE1 Dissemination Test Product Name Generator

Generates the list of filenames (and the separated parameters for them)
of the LE1 files that will be created and stored in the EAS for the
DSS LE1 Dissemination Test.

The output is a JSON file with a set of vectors, each of them addressed
with the following keywords:
 - "time": Vector of time stamps
 - "instrument": Vector of instrument tags (VIS, NIR or SIR)
 - "activity": Activity performed that produces the file
 - "filename": Science data file name (with extension .fits)
 - "size": Approx. estimated size (GB)

"""
#----------------------------------------------------------------------

import os
import sys
import json

from pprint import pprint
from datetime import datetime, timedelta

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

def configureLogs(lvl, logfile):
    """
    Function to configure the output of the log system, to be used across the
    entire application.
    :param lvl: Log level for the console log handler
    :return: -
    """
    logger.setLevel(logging.DEBUG)

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
    logger.addHandler(c_handler)
    logger.addHandler(f_handler)
    lmodules = os.getenv('LOGGING_MODULES', default='').split(':')
    for lname in reversed(lmodules):
        lgr = logging.getLogger(lname)
        if not lgr.handlers:
            lgr.addHandler(c_handler)
            lgr.addHandler(f_handler)
        # logger.debug('Configuring logger with id "{}"'.format(lname))

class LE1_Sequence_ProdGen:
    """
    Class LE1_Sequence_ProdGen

    Generates the required product names for the given activities in a sequence
    """
    DeltaTime = {"NS-1": 7.5,
                 "NS-2": 7.5,
                 "NS-3": 7.5,
                 "NS-4": 7.5,
                 "SS": 745,
                 "TP": 2698.5 - 2010.5,
                 "DK": 3838.5 - 3020.5,
                 "CH": 8103.5 - 7285.5,
                 "BI": 11473 - 10536,
                 "FF": 12511 - 11546,
                 "R": 20,
                 "Y": 627,
                 "J": 758,
                 "H": 889,
                 "D": 1000}
    Description = {"NS-1": 'NominalScience',
                   "NS-2": 'NominalScience',
                   "NS-3": 'NominalScience',
                   "NS-4": 'NominalScience',
                   "SS": 'ShortScience',
                   "TP": 'TrapPumping',
                   "DK": 'Dark',
                   "CH": 'ChargeInjection',
                   "BI": 'Bias',
                   "FF": 'FlatField',
                   "R": '',
                   "Y": '',
                   "J": '',
                   "H": '',
                   "D": ''}

    t0 = None

    def __init__(self, obsid, dither, fld_id, seqid, seq, start_time, lon, lat, angle):
        """
        Initializes instance
        """
        self.obsid = obsid
        self.dither = dither
        self.fieldid = fld_id
        self.seqid = seqid
        self.seq = seq
        self.start_time = datetime.fromisoformat(start_time)
        self.lon = lon
        self.lat = lat
        self.angle = angle
        self.t = self.start_time
        self.pt = 1

    @classmethod
    def resetTime(cls, start_time):
        """
        Uses current time as t0
        """
        LE1_Sequence_ProdGen.t0 = datetime.fromisoformat(start_time)

    def process(self):
        """
        Process sequence
        """
        seq = {"time": [],
               "obs": [],
               "activity": [],
               "point": [],
               "file": []}

        subseq = 1
        acts = self.seq.split('_')
        for act in acts:
            if act.startswith('R'):
                generator = self.gen_NISP
                subseq = 1
            elif act == 'D':
                generator = self.gen_D
            else:
                generator = self.gen_VIS

            prods = generator(act)
            for p in prods:
                t, dt, i, a, n, s = p
                seq["time"].append((t, dt))
                seq["obs"].append((self.obsid, self.fieldid, self.seqid, self.dither, subseq, len(acts) + 3))
                seq["activity"].append((i, a))
                seq["point"].append((self.lon, self.lat, self.angle))
                seq["file"].append((n, s))
                subseq = subseq + 1

        return seq

    def gen_VIS(self, act):
        """
        Generate NISP products
        """
        if act == "NS": act = f'NS-{self.dither}'
        return [ self.gen_prodName("VIS", act) + (1.3,) ]

    def gen_NISP(self, act):
        """
        Generate NISP products
        """
        return [ self.gen_prodName("SIR", 'R', '') + (0.6,),
                 self.gen_prodName("NIR", 'Y', '') + (0.6,),
                 self.gen_prodName("NIR", 'J', '') + (0.6,),
                 self.gen_prodName("NIR", 'H', '') + (0.6,) ]

    def gen_D(self, act):
        """
        Generate separated dark
        """
        return [ self.gen_prodName("NIR", 'D', '_Dark') + (0.6,) ]

    def gen_prodName(self, inst, act, comment=None):
        """
        Generate the actual product file name
        """
        delta = LE1_Sequence_ProdGen.DeltaTime[act]
        self.prod_time = self.start_time + timedelta(seconds=delta)

        if comment is None:
            comment = '_' + LE1_Sequence_ProdGen.Description[act]

        if inst == 'NIR' and act != 'D':
            pn1 = f'EUC_LE1_{inst}-{self.obsid}-{self.dither}-{act}'
        else:
            pn1 = f'EUC_LE1_{inst}-{self.obsid}-{self.dither}'

        date = self.prod_time.strftime("%Y%m%dT%H%M%S.%f")[:-3] +  'Z'
        dt = self.prod_time - LE1_Sequence_ProdGen.t0

        pn2 = f'{comment}_{date}_01.00.fits'

        return (date, dt.total_seconds(), inst, act, pn1 + pn2)


def main():
    """
    Get activities from activities.json (generated from OSS file), and
    generate file names and other data for each product to be generated
    and ingested into the EAS.
    """

    # Prepare logging system
    configureLogs(logging.INFO, 'le1_disstest_gen.log')

    # Get activities
    with open('activities.json') as fa:
        activities = json.load(fa)

    products = {"time": [],
                "obs": [],
                "activity": [],
                "file": [],
                "point": []}

    # Process all activities for each activities tag, generating the expected
    # file names according to the expected products
    k = 0
    for act in activities["acts"]:

        obsid = act["obs"]
        dither = act["dither"]
        fldid = act["field"]
        seqid = act["seq_id"]
        seq = act["act"]
        start_time = act["start"]
        lon = act["lon"]
        lat = act["lat"]
        angle = act["angle"]

        g = LE1_Sequence_ProdGen(obsid=obsid, dither=dither,
                                 fld_id=fldid, seqid=seqid,
                                 seq=seq, start_time=start_time,
                                 lon=lon, lat=lat, angle=angle)
        if k < 1:
            LE1_Sequence_ProdGen.resetTime(start_time)
            k = k + 1

        results = g.process()

        # Save results in general output structure and
        for col in results.keys():
            products[col].extend(results[col])

        # dump outputs to console
        logger.info(f'At {start_time} - {obsid}-{dither} ({seqid}) - {seq}')
        for i,t in enumerate(results["time"]):
            logger.info(f'{k} : {t} {results["obs"][i]} ' +
                        f'{results["activity"][i]} {results["file"][i]}')
            k = k + 1

    # Get some stats
    datestrTodateTime = lambda t: datetime.strptime(t[:-1], "%Y%m%dT%H%M%S.%f")
    mint = min(map(datestrTodateTime, [t[0] for t in products["time"]]))
    maxt = max(map(datestrTodateTime, [t[0] for t in products["time"]]))
    minobs = min(map(lambda x : int(x), [o[0] for o in products["obs"]]))
    maxobs = max(map(lambda x : int(x), [o[0] for o in products["obs"]]))
    ndays = (maxt - mint).total_seconds() / 86400.0

    products["summary"] = {
        "mint": mint.strftime("%Y%m%dT%H%M%S.%f")[:-3] + 'Z',
        "maxt": maxt.strftime("%Y%m%dT%H%M%S.%f")[:-3] + 'Z',
        "minobs": minobs,
        "maxobs": maxobs,
        "days": ndays,
        "obs_per_day": round((maxobs - minobs) / ndays),
        "npts": len(products["time"])
    }

    # Save output to file
    with open('le1_generated.json', 'w') as fg:
        json.dump(products, fg, indent=4)



if __name__ == '__main__':
    main()
