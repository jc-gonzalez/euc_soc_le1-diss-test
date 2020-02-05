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
import math

from pprint import pprint
from datetime import datetime, timedelta

from ingest_client_sc456 import main as ab_ingest_fn

#----------------------------------------------------------------------

_filedir_ = os.path.dirname(os.path.realpath(__file__))
_appsdir_, _ = os.path.split(_filedir_)
_basedir_, _ = os.path.split(_appsdir_)
sys.path.insert(0, os.path.abspath(os.path.join(_filedir_, _basedir_, _appsdir_)))

PYTHON2 = False
PY_NAME = "python3"

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

def date_to_jd(dt : datetime):
    """
    Convert a date to Julian Day.
    Algorithm from 'Practical Astronomy with your Calculator or Spreadsheet',
    year : int
        Year as integer. Years preceding 1 A.D. should be 0 or negative.
        The year before 1 A.D. is 0, 10 B.C. is year -9.
    month : int
        Month as integer, Jan = 1, Feb. = 2, etc.
    day : float
        Day, may contain fractional part.
    Returns
    -------
    jd : float
        Julian Day
    """

    tpl = dt.timetuple()
    year, month, day = tpl.tm_year, tpl.tm_mon, tpl.tm_mday
    hour, min, sec = tpl.tm_hour, tpl.tm_min, tpl.tm_sec

    if month == 1 or month == 2:
        yearp = year - 1
        monthp = month + 12
    else:
        yearp = year
        monthp = month

    # this checks where we are in relation to October 15, 1582, the beginning
    # of the Gregorian calendar.
    if ((year < 1582) or
            (year == 1582 and month < 10) or
            (year == 1582 and month == 10 and day < 15)):
        # before start of Gregorian calendar
        B = 0
    else:
        # after start of Gregorian calendar
        A = math.trunc(yearp / 100.)
        B = 2 - A + math.trunc(A / 4.)

    if yearp < 0:
        C = math.trunc((365.25 * yearp) - 0.75)
    else:
        C = math.trunc(365.25 * yearp)

    D = math.trunc(30.6001 * (monthp + 1))
    jd = B + C + D + day + 1720994.5

    h = hour + min / 60.0 + sec / 3600.0
    return jd + h / 24.0


def date_to_mjd(dt : datetime):
    """
    Convert datetime object to mjd
    """
    return date_to_jd(dt) - 2400000.5


class LE1_Metadata_Creator:
    """
    Class LE1 Metadata Creator

    Base class to generate the XML metadata file for a LE1 product, according
    to a predefined internal template
    """

    def __init__(self, logger):
        self.logger = logger
        self.init()

    def init(self):
        pass

    def setTemplate(self, tplFile):
        """
        Initialize the template string
        """
        try:
            with open(tplFile) as ftpl:
                self.tpl_string = ftpl.read()
        except IOError as e:
            logger.fatal('I/O error({0}): {1}'.format(e.errno, e.strerror))
        except:  # handle other exceptions such as attribute errors
            logger.fatal('Unexpected error: {}'.format(sys.exc_info()[0]))

        self.reset()

    def reset(self):
        self.content = self.tpl_string

    def set(self, item, value):
        """
        Sets the string '{{item}}' in the template with the value 'value'
        """
        sitem = '{{' + f'{item}' + '}}'
        self.content = self.content.replace(sitem, str(value))
        return self

    def setProduct(self, prodId, prodType, fileName):
        """
        Set Product Id and Type
        """
        return self.set('ProductId', prodId).\
                   set('ProductType', prodType). \
                   set('FileName', fileName)

    def setSW(self, name='LE1DissTest', release='0.1'):
        """
        Set software info
        """
        return self.set('SoftwareName', name).set('SoftwareRelease', release)

    def setDates(self, sdate):
        """
        Set the appropriate dates
        """
        str2date = lambda t: datetime.strptime(t[:-1], "%Y%m%dT%H%M%S.%f")
        date2str = lambda t: t.strftime("%Y%m%dT%H%M%S.%f")[:-3] + 'Z'

        date = str2date(sdate)
        sdateExpiration = date2str(date + timedelta(days=365*50))
        sdateMjd = date_to_mjd(date)

        return self.set('ExpDate', sdateExpiration).\
                   set('CreationDate', sdate).\
                   set('OBT', sdate).\
                   set('UTC', sdate).\
                   set('MJD', sdateMjd).\
                   set('StartTime', sdate)

    def setObsInfo(self, field, obs, point, dither, exps, totexp,
                   imode='Science', omode='ScienceWide'):
        """
        Set observation parameters
        """
        return self.set('FieldId', field).\
                   set('ObservationId', obs).\
                   set('DitherObservation', point).\
                   set('PointingId', dither).\
                   set('Exposure', exps).\
                   set('TotalExposure', totexp).\
                   set('InstrumentMode', imode).\
                   set('ObservationMode',omode)

    def setPointing(self, ra, dec, orient=0.0):
        """
        Set obs. pointing parameters
        """
        return self.set('RA', ra).\
                   set('Dec', dec).\
                   set('Orientation', orient).\
                   set('RA', ra).\
                   set('Dec', dec).\
                   set('Orientation', orient)

    def setReadoutAndStatuses(self):
        pass


class LE1_VIS_Metadata_Creator(LE1_Metadata_Creator):
    """
    Class LE1 VIS Metadata Creator

    Class to generate the XML metadata file for a LE1 VIS product, according
    to a predefined internal template
    """
    TemplateFile = os.path.join(_filedir_, 'tpl/le1_vis_meta_tpl.xml')

    def init(self):
        """
        Initialize the template string
        """
        self.setTemplate(LE1_VIS_Metadata_Creator.TemplateFile)

    def setReadoutAndStatuses(self, readout='NominalScience', shtstatus='OPEN',
                              calstatus='false', chgstatus='false'):
        """
        Changes readout mode and statuses
        """
        return self.set('ReadoutModeMethod', readout).\
                   set('ShtStatus', shtstatus).\
                   set('CalStatus', calstatus).\
                   set('ChgStatus', chgstatus)


class LE1_NIR_Metadata_Creator(LE1_Metadata_Creator):
    """
    Class LE1 Metadata Creator

    Class to generate the XML metadata file for a LE1 product, according
    to a predefined internal template
    """
    TemplateFile = os.path.join(_filedir_, 'tpl/le1_nir_meta_tpl.xml')

    def init(self):
        """
        Initialize the template string
        """
        self.setTemplate(LE1_NIR_Metadata_Creator.TemplateFile)

    def setReadoutAndStatuses(self, readout='NominalScience',
                              calstatus='false', grism='', filterpos=''):
        """
        Changes readout mode and statuses
        """
        return self.set('ReadoutModeMethod', readout).\
                   set('CalStatus', calstatus).\
                   set('GrismWheel', grism).\
                   set('FilterPos', filterpos)


class LE1_SIR_Metadata_Creator(LE1_Metadata_Creator):
    """
    Class LE1 Metadata Creator

    Class to generate the XML metadata file for a LE1 product, according
    to a predefined internal template
    """
    TemplateFile = os.path.join(_filedir_, 'tpl/le1_sir_meta_tpl.xml')

    def init(self):
        """
        Initialize the template string
        """
        self.setTemplate(LE1_SIR_Metadata_Creator.TemplateFile)

    def setReadoutAndStatuses(self, readout='NominalScience',
                              calstatus='false', grism='', filterpos=''):
        """
        Changes readout mode and statuses
        """
        return self.set('ReadoutModeMethod', readout).\
                   set('CalStatus', calstatus).\
                   set('GrismWheel', grism).\
                   set('FilterPos', filterpos)


def main():
    """
    Sample usage
    """
    import logging
    logger = logging.getLogger()

    # Define product id and type
    prodType = 'EUC_LE1_VIS'
    prodInstance = '11140-1-W-ChrInjFromLab'
    prodDate = '20180207T121254.0Z'
    prodVersion = '01.00'

    prodId = f'{prodType}-{prodInstance}_{prodVersion}'
    prodFile = f'{prodType}-{prodInstance}_{prodDate}_{prodVersion}.fits'

    # Create metadata creator
    le1meta = LE1_VIS_Metadata_Creator(logger)

    # Set parameter values
    le1meta.setProduct(prodId, prodType, prodFile).\
            setSW().\
            setDates(prodDate).\
            setObsInfo(1, '00010', '13121', 1, 2, 4).\
            setPointing(45.5, 12.4).\
            setReadoutAndStatuses('Calibration', 'CLOSED', 'true', 'false')

    with open('generated_le1_meta.xml', 'w') as fxml:
        fxml.write(le1meta.content)


if __name__ == '__main__':
    main()
