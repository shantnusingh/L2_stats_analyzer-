#  -*- coding: utf-8 -*-
"""
Created on Fri Nov 26 15:27:33 2021

@author: Harsh
"""
from configparser import ConfigParser
import re
import numpy as np
import pandas as pd
import os
import glob
import sys
import xlsxwriter

'''
class for du statistics, constructor initializes objects from config.ini file.
'''
class DuStatParser:
    def __init__(self):
        self.config = ConfigParser()
        self.config.read('config.ini')
        if 'stats_path' in self.config['DEFAULT']:
            self.path = self.config['DEFAULT']['stats_path']
        else:
            raise KeyError("The 'stats_path' key is missing in the config file.")
        self.setup = self.config['DEFAULT']['setup_name']
        self.num_cc = int(self.config['DEFAULT']['num_cc'])
        self.crnti = self.config['DEFAULT']['crnti']
        print(self.crnti)
        if self.config.has_option('DEFAULT','csv_save_to_dir'):
            self.csv_out_path = self.config['DEFAULT']['csv_save_to_dir']
        else:
            self.csv_out_path = False
        
    
    '''
    method for extracting timestamp from du stat logs of du.
    '''
    
    def GetTimeStamp(self, filename):
        rtime = re.compile(r'(?<=GNB DU Statistics).+?\s\w{3}(.+)', re.MULTILINE)
        return np.fromregex(filename, rtime, [('timestamp', np.str_, 32)])
    
    '''
    method for extracting long term bler from du statse
    '''
    
    def GetBlrStats(self, filename, cell):
        bler = re.compile(r'(?:LONG TERM BLER)(?:.|\s)+?<100\.0%(?:.|\s)+?(' + str(cell) + r')(?:.|\s)+?DL.+?(\d+\.\d+)(?:.|\s)+?UL.+?(\d+\.\d+)')
        return np.fromregex(filename, bler, [('cell_id', np.str_, 32), ('LTBDL%', np.float64), ('LTBUL%', np.float64)])
    
    '''
    method for extracting cell throughput from du stat file.
    '''
    
    def GetThptStats(self, filename, cell):
        cellthpt = re.compile(r'(?:  Cell Tpt Statistics)(?:.|\s)+?' + str(cell) + r'.+?(?P<num_of_ue>\d+\b).+?(?P<dl_thpt>\d+\.\d+\b).+?(?P<dl_retx_thpt>\d+\.\d+\b).+?(?P<dl_full_thpt>\d+\b).+?(?P<ul_thpt>\d+\.\d+\b)')
        return np.fromregex(filename, cellthpt, [('num_of_ue', np.int64), ('dl-thpt(mbps)', np.float64),('dl-retx-thpt(mbps)',np.float64), ('dl-full-thpt(mbps)', np.float64), ('ul-thpt(mbps)', np.float64)])
    
    '''
    method for creating csv file from extracted data for analysis.
    '''
    
    def GetCqiRiHist(self, filename, cell):
        # cqi_ri = re.compile(r'(?<=UE SCH:DL CQI & RI Histogram Statistics)(?:.|\s)(?:.+)(?:.|\s)(?:.+?DL-CQI-0)(?:.+)(?:.|\s)(?P<CRNTI>\d{1,})\s+(?P<Cell_Id>\d{1,})\s+(?P<DL_CQI_0>\d{1,})\s+(?P<DL_CQI_1>\d{1,})\s+(?P<DL_CQI_2>\d{1,})\s+(?P<DL_CQI_3>\d{1,})\s+(?P<DL_CQI_4>\d{1,})\s+(?P<DL_CQI_5>\d{1,})\s+(?P<DL_CQI_6>\d{1,})\s+(?P<DL_CQI_7>\d{1,})\s+(?P<DL_CQI_8>\d{1,})\s+(?P<DL_CQI_9>\d{1,})\s+(?P<DL_CQI_10>\d{1,})\s+(?P<DL_CQI_11>\d{1,})\s+(?P<DL_CQI_12>\d{1,})\s+(?P<DL_CQI_13>\d{1,})\s+(?P<DL_CQI_14>\d{1,})\s+', re.MULTILINE)
        # cqi_ri = re.compile(r'(?: UE SCH:DL CQI & RI Histogram Statistics)(?P<CRNTI>\d{1,})\s+(?P<Cell_Id>\d{1,})\s+(?P<DL_CQI_0>\d{1,})\s+(?P<DL_CQI_1>\d{1,})\s+(?P<DL_CQI_2>\d{1,})\s+(?P<DL_CQI_3>\d{1,})\s+(?P<DL_CQI_4>\d{1,})\s+(?P<DL_CQI_5>\d{1,})\s+(?P<DL_CQI_6>\d{1,})\s+(?P<DL_CQI_7>\d{1,})\s+(?P<DL_CQI_8>\d{1,})\s+(?P<DL_CQI_9>\d{1,})\s+(?P<DL_CQI_10>\d{1,})\s+(?P<DL_CQI_11>\d{1,})\s+(?P<DL_CQI_12>\d{1,})\s+(?P<DL_CQI_13>\d{1,})\s+(?P<DL_CQI_14>\d{1,})\s+(?P<DL_CQI_15>\d{1,})\s+(?P<DL_RPTD_RI_1>\d{1,})/(?P<DL_SCHD_RI_1>\d{1,})\s+(?P<DL_RPTD_RI_2>\d{1,})/(?P<DL_SCHD_RI_2>\d{1,})\s+(?P<DL_RPTD_RI_3>\d{1,})/(?P<DL_SCHD_RI_3>\d{1,})\s+(?P<DL_RPTD_RI_4>\d{1,})/(?P<DL_SCHD_RI_4>\d{1,})', re.MULTILINE)
        # cqi_ri = re.compile(r'(?<=UE SCH:DL CQI & RI Histogram Statistics)(?:.|\s)(?:.+)(?:.|\s)(?:.+?DL-CQI-0)(?:.+)(?:.|\s)(?P<CRNTI>\d{1,})\s+(?P<Cell_Id>\d{1,})\s+(?P<DL_CQI_0>\d{1,})\s+(?P<DL_CQI_1>\d{1,})\s+(?P<DL_CQI_2>\d{1,})\s+(?P<DL_CQI_3>\d{1,})\s+(?P<DL_CQI_4>\d{1,})\s+(?P<DL_CQI_5>\d{1,})\s+(?P<DL_CQI_6>\d{1,})\s+(?P<DL_CQI_7>\d{1,})\s+(?P<DL_CQI_8>\d{1,})\s+(?P<DL_CQI_9>\d{1,})\s+(?P<DL_CQI_10>\d{1,})\s+(?P<DL_CQI_11>\d{1,})\s+(?P<DL_CQI_12>\d{1,})\s+(?P<DL_CQI_13>\d{1,})\s+(?P<DL_CQI_14>\d{1,})\s+', re.MULTILINE)
        cqi_ri = re.compile(r'(?<=UE SCH:DL CQI & RI Histogram Statistics)(?:.|\s)(?:.+)(?:.|\s)(?:.+?DL-CQI-0)(?:.+)(?:.|\s)(?P<CRNTI>\d{1,})\s+(?P<Cell_Id>\d{1,})\s+(?P<DL_CQI_0>\d{1,})\s+(?P<DL_CQI_1>\d{1,})\s+(?P<DL_CQI_2>\d{1,})\s+(?P<DL_CQI_3>\d{1,})\s+(?P<DL_CQI_4>\d{1,})\s+(?P<DL_CQI_5>\d{1,})\s+(?P<DL_CQI_6>\d{1,})\s+(?P<DL_CQI_7>\d{1,})\s+(?P<DL_CQI_8>\d{1,})\s+(?P<DL_CQI_9>\d{1,})\s+(?P<DL_CQI_10>\d{1,})\s+(?P<DL_CQI_11>\d{1,})\s+(?P<DL_CQI_12>\d{1,})\s+(?P<DL_CQI_13>\d{1,})\s+(?P<DL_CQI_14>\d{1,})\s+(?P<DL_CQI_15>\d{1,})\s+\[(?P<DL_RPTD_RI_1>\d{1,})\s*/\s*(?P<DL_SCHD_RI_1>\d{1,})\s*\]\s*\[(?P<DL_RPTD_RI_2>\d{1,})\s*/\s*(?P<DL_SCHD_RI_2>\d{1,})\s*\]\s*\[(?P<DL_RPTD_RI_3>\d{1,})\s*/\s*(?P<DL_SCHD_RI_3>\d{1,})\s*\]\s*\[(?P<DL_RPTD_RI_4>\d{1,})\s*/\s*(?P<DL_SCHD_RI_4>\d{1,})\s*\]', re.MULTILINE)
        # cqi_ri = re.compile(r'UE SCH:DL CQI & RI Histogram Statistics(?:.|\s)+?DL-CQI-0(?:.|\s)+?(?P<CRNTI>\d+)\s+(?P<Cell_Id>\d+)\s+(?P<DL_CQI_0>\d+)\s+(?P<DL_CQI_1>\d+)\s+(?P<DL_CQI_2>\d+)\s+(?P<DL_CQI_3>\d+)\s+(?P<DL_CQI_4>\d+)\s+(?P<DL_CQI_5>\d+)\s+(?P<DL_CQI_6>\d+)\s+(?P<DL_CQI_7>\d+)\s+(?P<DL_CQI_8>\d+)\s+(?P<DL_CQI_9>\d+)\s+(?P<DL_CQI_10>\d+)\s+(?P<DL_CQI_11>\d+)\s+(?P<DL_CQI_12>\d+)\s+(?P<DL_CQI_13>\d+)\s+(?P<DL_CQI_14>\d+)\s+(?P<DL_CQI_15>\d+)\s+\[(?P<DL_RPTD_RI_1>\d+)/(?P<DL_SCHD_RI_1>\d+)\]\s+\[(?P<DL_RPTD_RI_2>\d+)/(?P<DL_SCHD_RI_2>\d+)\]\s+\[(?P<DL_RPTD_RI_3>\d+)/(?P<DL_SCHD_RI_3>\d+)\]\s+\[(?P<DL_RPTD_RI_4>\d+)/(?P<DL_SCHD_RI_4>\d+)\]', re.MULTILINE)
        # cqi_ri = re.compile(
        # r'UE SCH:DL CQI & RI Histogram Statistics\s+'
        # r'UE-ID\s+CELL-ID\s+DL-CQI-0\s+DL-CQI-1\s+DL-CQI-2\s+DL-CQI-3\s+DL-CQI-4\s+DL-CQI-5\s+DL-CQI-6\s+DL-CQI-7\s+DL-CQI-8\s+DL-CQI-9\s+DL-CQI-10\s+DL-CQI-11\s+DL-CQI-12\s+DL-CQI-13\s+DL-CQI-14\s+DL-CQI-15\s+\[DL-RPTD-RI-1/DL-SCHD-RI-1\]\s+\[DL-RPTD-RI-2/DL-SCHD-RI-2\]\s+\[DL-RPTD-RI-3/DL-SCHD-RI-3\]\s+\[DL-RPTD-RI-4/DL-SCHD-RI-4\]\s+'
        # r'(?P<CRNTI>\d+)\s+(?P<Cell_Id>\d+)\s+(?P<DL_CQI_0>\d+)\s+(?P<DL_CQI_1>\d+)\s+(?P<DL_CQI_2>\d+)\s+(?P<DL_CQI_3>\d+)\s+(?P<DL_CQI_4>\d+)\s+(?P<DL_CQI_5>\d+)\s+(?P<DL_CQI_6>\d+)\s+(?P<DL_CQI_7>\d+)\s+(?P<DL_CQI_8>\d+)\s+(?P<DL_CQI_9>\d+)\s+(?P<DL_CQI_10>\d+)\s+(?P<DL_CQI_11>\d+)\s+(?P<DL_CQI_12>\d+)\s+(?P<DL_CQI_13>\d+)\s+(?P<DL_CQI_14>\d+)\s+(?P<DL_CQI_15>\d+)\s+\[(?P<DL_RPTD_RI_1>\d+)/(?P<DL_SCHD_RI_1>\d+)\]\s+\[(?P<DL_RPTD_RI_2>\d+)/(?P<DL_SCHD_RI_2>\d+)\]\s+\[(?P<DL_RPTD_RI_3>\d+)/(?P<DL_SCHD_RI_3>\d+)\]\s+\[(?P<DL_RPTD_RI_4>\d+)/(?P<DL_SCHD_RI_4>\d+)\]',
        # re.MULTILINE
    # )
        print("Getting CQI")
        return np.fromregex(filename, cqi_ri, [
        ('CRNTI', np.int64), ('Cell_Id', np.int64),
        ('DL_CQI_0', np.int64), ('DL_CQI_1', np.int64), ('DL_CQI_2', np.int64),
        ('DL_CQI_3', np.int64), ('DL_CQI_4', np.int64), ('DL_CQI_5', np.int64),
        ('DL_CQI_6', np.int64), ('DL_CQI_7', np.int64), ('DL_CQI_8', np.int64),
        ('DL_CQI_9', np.int64), ('DL_CQI_10', np.int64), ('DL_CQI_11', np.int64),
        ('DL_CQI_12', np.int64), ('DL_CQI_13', np.int64), ('DL_CQI_14', np.int64),
        ('DL_CQI_15', np.int64),
        ('DL_RPTD_RI_1', np.int64), ('DL_SCHD_RI_1', np.int64),
        ('DL_RPTD_RI_2', np.int64), ('DL_SCHD_RI_2', np.int64),
        ('DL_RPTD_RI_3', np.int64), ('DL_SCHD_RI_3', np.int64),
        ('DL_RPTD_RI_4', np.int64), ('DL_SCHD_RI_4', np.int64),
    ])
    
    def GetCellInstStats(self, filename, cell):
        reg = re.compile(
        r'(?<=Cell Instantaneous Statistics)(?:.|\s)+?'
        r'(?P<cell_id>' + str(cell) + r')\s+'
        r'(?P<NR_MU>\d+)\s+'
        r'(?P<MODE>\w+)\s+'
        r'(?P<MAX_SZ_RNG_DL>\d+)\s+'
        r'(?P<MAX_SZ_RNG_UL>\d+)\s+'
        r'(?P<AVG_DL_PRB>\d+)\s+'
        r'(?P<AVG_OCNS_PRB>\d+)\s+'
        r'(?P<AVG_TOTAL_DL_PRB>\d+)\s+'
        r'(?P<DL_OCC>\d+)\s+'
        r'(?P<OCNS_OCC>\d+)\s+'
        r'(?P<DL_SUCC>\d+)\s+'
        r'(?P<DL_FAIL_HQ>\d+)\s+'
        r'(?P<DL_FAIL_F0_LMT_HIT>\d+)\s+'
        r'(?P<DL_FAIL_F2_LMT_HIT>\d+)\s+'
        r'(?P<DL_FAIL_F0F2_LMT_HIT>\d+)\s+'
        r'(?P<DL_RES_SET_UNAVAIL>\d+)\s+'
        r'(?P<DL_MON_IDX_FAIL>\d+)\s+'
        r'(?P<UL_HQ_RETX_DROP>\d+)\s+'
        r'(?P<MSG3_RETX_DROP>\d+)\s+'
        r'(?P<DL_TRY_SEL>\d+)\s+'
        r'(?P<DL_FAIL_SEL>\d+)\s+'
        r'(?P<DL_FAIL_SEL1>\d+)\s+'
        r'(?P<DL_FAIL_SEL2>\d+)\s+'
        r'(?P<DL_FAIL_SEL3>\d+)\s+'
        r'(?P<DL_FAIL_SEL4>\d+)\s+'
        r'(?P<DL_FAIL_SEL5>\d+)\s+'
        r'(?P<DL_FAIL_SEL6>\d+)\s+'
        r'(?P<DL_FAIL_SEL7>\d+)\s+'
        r'(?P<DL_FAIL_SEL8>\d+)\s+'
        r'(?P<DL_FAIL_SEL9>\d+)\s+'
        r'(?P<DL_TRY_CSI>\d+)\s+'
        r'(?P<DL_FAIL_CSI>\d+)\s+'
        r'(?P<DL_FAIL_CSI1>\d+)\s+'
        r'(?P<DL_FAIL_CSI2>\d+)\s+'
        r'(?P<DL_FAIL_CSI3>\d+)\s+'
        r'(?P<DL_FAIL_CSI4>\d+)\s+'
        r'(?P<DL_FAIL_CSI5>\d+)\s+'
        r'(?P<DL_FAIL_CSI6>\d+)\s+'
        r'(?P<DL_FAIL_CSI7>\d+)\s+'
        r'(?P<DL_FAIL_CSI8>\d+)\s+'
        r'(?P<DL_FAIL_CSI9>\d+)\s+'
        r'(?P<DL_FAIL_CSI10>\d+)\s+'
        r'(?P<DL_FAIL_SR_OVERLAP>\d+)\s+'
        r'(?P<DL_FAIL_CSI_OVERLAP>\d+)\s+'
        r'(?P<DL_FAIL_OTHER>\d+)\s+'
        r'(?P<DL_NEWTX>\d+)\s+'
        r'(?P<DL_RETX>\d+)\s+'
        r'(?P<DL_DTX>\d+)\s+'
        r'(?P<DL_BLER>\d+)\s+'
        r'(?P<AVG_UL_PRB>\d+)\s+'
        r'(?P<AVG_TOTAL_UL_PRB>\d+)\s+'
        r'(?P<UL_OCC>\d+)\s+'
        r'(?P<UL_SUCC>\d+)\s+'
        r'(?P<UL_NEWTX>\d+)\s+'
        r'(?P<UL_RETX>\d+)\s+'
        r'(?P<UL_DTX>\d+)\s+'
        r'(?P<UL_DATIND>\d+)\s+'
        r'(?P<UL_BLER>\d+)\s+'
        r'(?P<TTI_ERR_MAJOR>\d+)\s+'
        r'(?P<TTI_ERR_MINOR>\d+)\s+'
        r'(?P<TTI_NO_DATA_SCH>\d+)\s+'
        r'(?P<DL_NACK_RV_0>\d+)\s+'
        r'(?P<DL_NACK_RV_1>\d+)\s+'
        r'(?P<DL_NACK_RV_2>\d+)\s+'
        r'(?P<DL_NACK_RV_3>\d+)\s+'
        r'(?P<DL_DTX_RV_0>\d+)\s+'
        r'(?P<DL_DTX_RV_1>\d+)\s+'
        r'(?P<DL_DTX_RV_2>\d+)\s+'
        r'(?P<DL_DTX_RV_3>\d+)\s+'
        r'(?P<PER_PRB_DLMCS>\d+\.\d+)\s+'
        r'(?P<PER_OCC_DLMCS>\d+\.\d+)\s+'
        r'(?P<PER_PRB_ULMCS>\d+\.\d+)\s+'
        r'(?P<PER_OCC_ULMCS>\d+\.\d+)\s+'
        r'(?P<MBWP_NOT_SUPP_BY_UE>\d+)\s+'
        r'(?P<MBWP_SIZE_NOT_SUPP_BY_UE>\d+)\s+'
        r'(?P<UL_FAIL_0>\d+)\s+'
        r'(?P<UL_FAIL_1>\d+)\s+'
        r'(?P<UL_FAIL_2>\d+)\s+'
        r'(?P<UL_FAIL_3>\d+)\s+'
        r'(?P<UL_FAIL_4>\d+)\s+'
        r'(?P<UL_FAIL_5>\d+)\s+'
        r'(?P<UL_FAIL_6>\d+)\s+'
        r'(?P<UL_FAIL_7>\d+)\s+'
        r'(?P<UL_FAIL_8>\d+)\s+'
        r'(?P<UL_FAIL_9>\d+)\s+'
        r'(?P<UL_FAIL_10>\d+)\s+'
        r'(?P<UL_FAIL_11>\d+)\s+'
        r'(?P<UL_FAIL_12>\d+)\s+'
        r'(?P<UL_FAIL_13>\d+)\s+'
        r'(?P<UL_FAIL_14>\d+)\s+'
        r'(?P<UL_FAIL_15>\d+)\s+'
        r'(?P<UL_FAIL_16>\d+)\s+'
        r'(?P<UL_FAIL_17>\d+)\s+'
        r'(?P<UL_FAIL_18>\d+)\s+'
        r'(?P<UL_FAIL_19>\d+)\s+'
        r'(?P<UL_FAIL_20>\d+)\s+'
        r'(?P<UL_FAIL_21>\d+)',
        re.MULTILINE
    )
        return np.fromregex(filename, reg, [
        ('cell_id', np.int64),
        ('NR_MU', np.int64),
        ('MODE', 'U3'),  # String of length 3 for TDD
        ('MAX_SZ_RNG_DL', np.int64),
        ('MAX_SZ_RNG_UL', np.int64),
        ('AVG_DL_PRB', np.int64),
        ('AVG_OCNS_PRB', np.int64),
        ('AVG_TOTAL_DL_PRB', np.int64),
        ('DL_OCC', np.int64),
        ('OCNS_OCC', np.int64),
        ('DL_SUCC', np.int64),
        ('DL_FAIL_HQ', np.int64),
        ('DL_FAIL_F0_LMT_HIT', np.int64),
        ('DL_FAIL_F2_LMT_HIT', np.int64),
        ('DL_FAIL_F0F2_LMT_HIT', np.int64),
        ('DL_RES_SET_UNAVAIL', np.int64),
        ('DL_MON_IDX_FAIL', np.int64),
        ('UL_HQ_RETX_DROP', np.int64),
        ('MSG3_RETX_DROP', np.int64),
        ('DL_TRY_SEL', np.int64),
        ('DL_FAIL_SEL', np.int64),
        ('DL_FAIL_SEL1', np.int64),
        ('DL_FAIL_SEL2', np.int64),
        ('DL_FAIL_SEL3', np.int64),
        ('DL_FAIL_SEL4', np.int64),
        ('DL_FAIL_SEL5', np.int64),
        ('DL_FAIL_SEL6', np.int64),
        ('DL_FAIL_SEL7', np.int64),
        ('DL_FAIL_SEL8', np.int64),
        ('DL_FAIL_SEL9', np.int64),
        ('DL_TRY_CSI', np.int64),
        ('DL_FAIL_CSI', np.int64),
        ('DL_FAIL_CSI1', np.int64),
        ('DL_FAIL_CSI2', np.int64),
        ('DL_FAIL_CSI3', np.int64),
        ('DL_FAIL_CSI4', np.int64),
        ('DL_FAIL_CSI5', np.int64),
        ('DL_FAIL_CSI6', np.int64),
        ('DL_FAIL_CSI7', np.int64),
        ('DL_FAIL_CSI8', np.int64),
        ('DL_FAIL_CSI9', np.int64),
        ('DL_FAIL_CSI10', np.int64),
        ('DL_FAIL_SR_OVERLAP', np.int64),
        ('DL_FAIL_CSI_OVERLAP', np.int64),
        ('DL_FAIL_OTHER', np.int64),
        ('DL_NEWTX', np.int64),
        ('DL_RETX', np.int64),
        ('DL_DTX', np.int64),
        ('DL_BLER', np.int64),
        ('AVG_UL_PRB', np.int64),
        ('AVG_TOTAL_UL_PRB', np.int64),
        ('UL_OCC', np.int64),
        ('UL_SUCC', np.int64),
        ('UL_NEWTX', np.int64),
        ('UL_RETX', np.int64),
        ('UL_DTX', np.int64),
        ('UL_DATIND', np.int64),
        ('UL_BLER', np.int64),
        ('TTI_ERR_MAJOR', np.int64),
        ('TTI_ERR_MINOR', np.int64),
        ('TTI_NO_DATA_SCH', np.int64),
        ('DL_NACK_RV_0', np.int64),
        ('DL_NACK_RV_1', np.int64),
        ('DL_NACK_RV_2', np.int64),
        ('DL_NACK_RV_3', np.int64),
        ('DL_DTX_RV_0', np.int64),
        ('DL_DTX_RV_1', np.int64),
        ('DL_DTX_RV_2', np.int64),
        ('DL_DTX_RV_3', np.int64),
        ('PER_PRB_DLMCS', np.float64),
        ('PER_OCC_DLMCS', np.float64),
        ('PER_PRB_ULMCS', np.float64),
        ('PER_OCC_ULMCS', np.float64),
        ('MBWP_NOT_SUPP_BY_UE', np.int64),
        ('MBWP_SIZE_NOT_SUPP_BY_UE', np.int64),
        ('UL_FAIL_0', np.int64),
        ('UL_FAIL_1', np.int64),
        ('UL_FAIL_2', np.int64),
        ('UL_FAIL_3', np.int64),
        ('UL_FAIL_4', np.int64),
        ('UL_FAIL_5', np.int64),
        ('UL_FAIL_6', np.int64),
        ('UL_FAIL_7', np.int64),
        ('UL_FAIL_8', np.int64),
        ('UL_FAIL_9', np.int64),
        ('UL_FAIL_10', np.int64),
        ('UL_FAIL_11', np.int64),
        ('UL_FAIL_12', np.int64),
        ('UL_FAIL_13', np.int64),
        ('UL_FAIL_14', np.int64),
        ('UL_FAIL_15', np.int64),
        ('UL_FAIL_16', np.int64),
        ('UL_FAIL_17', np.int64),
        ('UL_FAIL_18', np.int64),
        ('UL_FAIL_19', np.int64),
        ('UL_FAIL_20', np.int64),
        ('UL_FAIL_21', np.int64)
    ])


    def GetUeDrxInstStat(self, filename, cell, crnti):
        drx_stats = re.compile(
            r'UE SCH:DRX Instantaneous Statistics\s+'
            r'[-]+\s+'
            r'UE-ID\s+CELL-ID\s+BEAM-ID\s+UL-HQ-RETX-DROP\s+MSG3-RETX-DROP\s+STRT-ON-DUR\s+'
            r'STRT-DL-INACTV\s+STRT-UL-INACTV\s+STRT-DL-RETX\s+STRT-UL-RETX\s+EXP-ON-DUR\s+'
            r'EXP-INACTV\s+EXP-DL-RETX\s+EXP-UL-RETX\s+EXP-SHORT_CYCL\s+UE_INACT_TO_ACT\s+'
            r'UE_ACT_TO_INACT\s+UE_DL_INACT_CNT\s+UE_UL_INACT_CNT\s+UE_DL_SKIP_DRX_WAKEUP_CNT\s+'
            r'UE_DL_DRX_WAKEUP_SCH_CNT\s+UE_DL_DRX_WAKEUP_DTX_CNT\s+UE_UL_SKIP_DRX_WAKEUP_CNT\s+'
            r'UE_UL_DRX_WAKEUP_SCH_CNT\s+UE_UL_DRX_WAKEUP_DTX_CNT\s*\n+'
            r'(?P<crnti>' + str(crnti) + r')\s+'
            r'(?P<cell_id>' + str(cell) + r')\s+'
            r'(?P<BEAM_ID>\d+)\s+'
            r'(?P<UL_HQ_RETX_DROP>\d+)\s+'
            r'(?P<MSG3_RETX_DROP>\d+)\s+'
            r'(?P<STRT_ON_DUR>\d+)\s+'
            r'(?P<STRT_DL_INACTV>\d+)\s+'
            r'(?P<STRT_UL_INACTV>\d+)\s+'
            r'(?P<STRT_DL_RETX>\d+)\s+'
            r'(?P<STRT_UL_RETX>\d+)\s+'
            r'(?P<EXP_ON_DUR>\d+)\s+'
            r'(?P<EXP_INACTV>\d+)\s+'
            r'(?P<EXP_DL_RETX>\d+)\s+'
            r'(?P<EXP_UL_RETX>\d+)\s+'
            r'(?P<EXP_SHORT_CYCL>\d+)\s+'
            r'(?P<UE_INACT_TO_ACT>\d+)\s+'
            r'(?P<UE_ACT_TO_INACT>\d+)\s+'
            r'(?P<UE_DL_INACT_CNT>\d+)\s+'
            r'(?P<UE_UL_INACT_CNT>\d+)\s+'
            r'(?P<UE_DL_SKIP_DRX_WAKEUP_CNT>\d+)\s+'
            r'(?P<UE_DL_DRX_WAKEUP_SCH_CNT>\d+)\s+'
            r'(?P<UE_DL_DRX_WAKEUP_DTX_CNT>\d+)\s+'
            r'(?P<UE_UL_SKIP_DRX_WAKEUP_CNT>\d+)\s+'
            r'(?P<UE_UL_DRX_WAKEUP_SCH_CNT>\d+)\s+'
            r'(?P<UE_UL_DRX_WAKEUP_DTX_CNT>\d+)',
            re.MULTILINE
        )
        print("DRX Stats")

        return np.fromregex(filename, drx_stats, [
        ('crnti', np.int64),
        ('cell_id', np.int64),
        ('BEAM_ID', np.int64),
        ('UL_HQ_RETX_DROP', np.int64),
        ('MSG3_RETX_DROP', np.int64),
        ('STRT_ON_DUR', np.int64),
        ('STRT_DL_INACTV', np.int64),
        ('STRT_UL_INACTV', np.int64),
        ('STRT_DL_RETX', np.int64),
        ('STRT_UL_RETX', np.int64),
        ('EXP_ON_DUR', np.int64),
        ('EXP_INACTV', np.int64),
        ('EXP_DL_RETX', np.int64),
        ('EXP_UL_RETX', np.int64),
        ('EXP_SHORT_CYCL', np.int64),
        ('UE_INACT_TO_ACT', np.int64),
        ('UE_ACT_TO_INACT', np.int64),
        ('UE_DL_INACT_CNT', np.int64),
        ('UE_UL_INACT_CNT', np.int64),
        ('UE_DL_SKIP_DRX_WAKEUP_CNT', np.int64),
        ('UE_DL_DRX_WAKEUP_SCH_CNT', np.int64),
        ('UE_DL_DRX_WAKEUP_DTX_CNT', np.int64),
        ('UE_UL_SKIP_DRX_WAKEUP_CNT', np.int64),
        ('UE_UL_DRX_WAKEUP_SCH_CNT', np.int64),
        ('UE_UL_DRX_WAKEUP_DTX_CNT', np.int64)
        ])
    
    def GetUeLaHistStat(self, filename, cell, crnti):
        reg = re.compile(
        r'UE SCH: LA Histogram Statistics\s+'
        r'[-]+\s+'
        r'UE-ID\s+CELL-ID\s+DL-iBLER%\s+DL-rBLER%\s+DL-resBLER%\s+DL-tBLER%\s+DL-avgCQI\s+DL-avgMCS\s+DL-avgRptRI\s+DL-avgRI\s+UL-iBLER%\s+UL-rBLER%\s+UL-resBLER%\s+UL-tBLER%\s+UL-avgSNR\s+UL-avgMCS\s+UL-avgRI\s+UL-MinPrb-Mcs-Adj\s+UL-DcPrb-Mcs-Adj\s+DL-OLLA\s+DL-CH-AGE\s+UL-OLLA\s+UL-CH-AGE\s+.*\s+'
        r'(?P<crnti>' + str(crnti) + r')\s+'
        r'(?P<cell_id>' + str(cell) + r')\s+'
        r'(?P<DL_iBLER>\d+\.\d+)\s+'
        r'(?P<DL_rBLER>\d+\.\d+)\s+'
        r'(?P<DL_resBLER>\d+\.\d+)\s+'
        r'(?P<DL_tBLER>\d+\.\d+)\s+'
        r'(?P<DL_avgCQI>\d+\.\d+)\s+'
        r'(?P<DL_avgMCS>\d+\.\d+)\s+'
        r'(?P<DL_avgRptRI>\d+\.\d+)\s+'
        r'(?P<DL_avgRI>\d+\.\d+)\s+'
        r'(?P<UL_iBLER>\d+\.\d+)\s+'
        r'(?P<UL_rBLER>\d+\.\d+)\s+'
        r'(?P<UL_resBLER>\d+\.\d+)\s+'
        r'(?P<UL_tBLER>\d+\.\d+)\s+'
        r'(?P<UL_avgSNR>\d+\.\d+)\s+'
        r'(?P<UL_avgMCS>\d+\.\d+)\s+'
        r'(?P<UL_avgRI>\d+\.\d+)\s+'
        r'(?P<UL_MinPrb_Mcs_Adj>\d+)\s+'
        r'(?P<UL_DcPrb_Mcs_Adj>\d+)\s+'
        r'(?P<DL_OLLA>\d+)\s+'
        r'(?P<DL_CH_AGE>\d+)\s+'
        r'(?P<UL_OLLA>\d+)\s+'
        r'(?P<UL_CH_AGE>\d+)\s+'
        r'\[(?P<DL_RV_0>\d+)\s*/\s*(?P<DL_RV_0_ACK>\d+)\s*/\s*(?P<DL_RV_0_NACK>\d+)\s*/\s*(?P<DL_RV_0_DTX>\d+)\s*/\s*(?P<DL_RV_0_BLER>\d+\.\d+)\s*\]'
        r'\[(?P<DL_RV_1>\d+)\s*/\s*(?P<DL_RV_1_ACK>\d+)\s*/\s*(?P<DL_RV_1_NACK>\d+)\s*/\s*(?P<DL_RV_1_DTX>\d+)\s*/\s*(?P<DL_RV_1_BLER>\d+\.\d+)\s*\]'
        r'\[(?P<DL_RV_2>\d+)\s*/\s*(?P<DL_RV_2_ACK>\d+)\s*/\s*(?P<DL_RV_2_NACK>\d+)\s*/\s*(?P<DL_RV_2_DTX>\d+)\s*/\s*(?P<DL_RV_2_BLER>\d+\.\d+)\s*\]'
        r'\[(?P<DL_RV_3>\d+)\s*/\s*(?P<DL_RV_3_ACK>\d+)\s*/\s*(?P<DL_RV_3_NACK>\d+)\s*/\s*(?P<DL_RV_3_DTX>\d+)\s*/\s*(?P<DL_RV_3_BLER>\d+\.\d+)\s*\]'
        r'\[(?P<UL_RV_0>\d+)\s*/\s*(?P<UL_RV_0_ACK>\d+)\s*/\s*(?P<UL_RV_0_NACK>\d+)\s*/\s*(?P<UL_RV_0_BLER>\d+\.\d+)\s*\]'
        r'\[(?P<UL_RV_1>\d+)\s*/\s*(?P<UL_RV_1_ACK>\d+)\s*/\s*(?P<UL_RV_1_NACK>\d+)\s*/\s*(?P<UL_RV_1_BLER>\d+\.\d+)\s*\]'
        r'\[(?P<UL_RV_2>\d+)\s*/\s*(?P<UL_RV_2_ACK>\d+)\s*/\s*(?P<UL_RV_2_NACK>\d+)\s*/\s*(?P<UL_RV_2_BLER>\d+\.\d+)\s*\]'
        r'\[(?P<UL_RV_3>\d+)\s*/\s*(?P<UL_RV_3_ACK>\d+)\s*/\s*(?P<UL_RV_3_NACK>\d+)\s*/\s*(?P<UL_RV_3_BLER>\d+\.\d+)\s*\]'
        r'\[(?P<UL_Retx_PRB>\d+)\s*/\s*(?P<UL_Retx_PRB_ACK>\d+)\s*/\s*(?P<UL_Retx_PRB_NACK>\d+)\s*/\s*(?P<UL_Retx_PRB_BLER>\d+\.\d+)\s*\]'
        r'\[(?P<DL_Retx_PRB>\d+)\s*/\s*(?P<DL_Retx_PRB_ACK>\d+)\s*/\s*(?P<DL_Retx_PRB_NACK>\d+)\s*/\s*(?P<DL_Retx_PRB_DTX>\d+)\s*/\s*(?P<DL_Retx_PRB_BLER>\d+\.\d+)\s*\]'
        r'\s*(?P<PDCCH_LA_STEP_UP>\d+)\s+'
        r'(?P<PDCCH_LA_STEP_UP_AGGR_CHG>\d+)\s+'
        r'(?P<PDCCH_LA_STEP_DOWN>\d+)\s+'
        r'(?P<PDCCH_LA_STEP_DOWN_AGGR_CHG>\d+)\s+'
        r'(?P<PDCCH_LA_RESET>\d+)\s+'
        r'(?P<PDCCH_LA_CALC>\d+)',
        re.MULTILINE
    )
    
        return np.fromregex(filename, reg, [
        ('crnti', np.int64), ('cell_id', np.int64),
        ('DL_iBLER', np.float64), ('DL_rBLER', np.float64), 
        ('DL_resBLER', np.float64), ('DL_tBLER', np.float64),
        ('DL_avgCQI', np.float64), ('DL_avgMCS', np.float64),
        ('DL_avgRptRI', np.float64), ('DL_avgRI', np.float64),
        ('UL_iBLER', np.float64), ('UL_rBLER', np.float64),
        ('UL_resBLER', np.float64), ('UL_tBLER', np.float64),
        ('UL_avgSNR', np.float64), ('UL_avgMCS', np.float64),
        ('UL_avgRI', np.float64), ('UL_MinPrb_Mcs_Adj', np.int64),
        ('UL_DcPrb_Mcs_Adj', np.int64), ('DL_OLLA', np.int64),
        ('DL_CH_AGE', np.int64), ('UL_OLLA', np.int64),
        ('UL_CH_AGE', np.int64),
        ('DL_RV_0', np.int64),('DL_RV_0_ACK', np.int64), ('DL_RV_0_NACK', np.int64),
        ('DL_RV_0_DTX', np.int64), ('DL_RV_0_BLER', np.float64),
        ('DL_RV_1', np.int64),('DL_RV_1_ACK', np.int64), ('DL_RV_1_NACK', np.int64),
        ('DL_RV_1_DTX', np.int64), ('DL_RV_1_BLER', np.float64),
        ('DL_RV_2', np.int64),('DL_RV_2_ACK', np.int64), ('DL_RV_2_NACK', np.int64),
        ('DL_RV_2_DTX', np.int64), ('DL_RV_2_BLER', np.float64),
        ('DL_RV_3', np.int64),('DL_RV_3_ACK', np.int64), ('DL_RV_3_NACK', np.int64),
        ('DL_RV_3_DTX', np.int64), ('DL_RV_3_BLER', np.float64),
        ('UL_RV_0', np.int64),('UL_RV_0_ACK', np.int64), ('UL_RV_0_NACK', np.int64),
        ('UL_RV_0_BLER', np.float64),
        ('UL_RV_1', np.int64),('UL_RV_1_ACK', np.int64), ('UL_RV_1_NACK', np.int64),
        ('UL_RV_1_BLER', np.float64),
        ('UL_RV_2', np.int64),('UL_RV_2_ACK', np.int64), ('UL_RV_2_NACK', np.int64),
        ('UL_RV_2_BLER', np.float64),
        ('UL_RV_3', np.int64),('UL_RV_3_ACK', np.int64), ('UL_RV_3_NACK', np.int64),
        ('UL_RV_3_BLER', np.float64),
        ('UL_Retx_PRB', np.int64),('UL_Retx_PRB_ACK', np.int64), ('UL_Retx_PRB_NACK', np.int64),
        ('UL_Retx_PRB_BLER', np.float64),
        ('DL_Retx_PRB', np.int64),('DL_Retx_PRB_ACK', np.int64), ('DL_Retx_PRB_NACK', np.int64),
        ('DL_Retx_PRB_DTX', np.int64), ('DL_Retx_PRB_BLER', np.float64),
        ('PDCCH_LA_STEP_UP', np.int64),
        ('PDCCH_LA_STEP_UP_AGGR_CHG', np.int64),
        ('PDCCH_LA_STEP_DOWN', np.int64),
        ('PDCCH_LA_STEP_DOWN_AGGR_CHG', np.int64),
        ('PDCCH_LA_RESET', np.int64),
        ('PDCCH_LA_CALC', np.int64),
    ])

    def GetDlMcsHistStat(self, filename, cell, crnti):
        reg = re.compile(
            r'UE SCH:DL MCS Histogram Statistics\s+'
            r'[-]+\s+'
            r'UE-ID\s+CELL-ID\s+\[DL-MCS-0.*\]\[DL-MCS-1.*\].*\s+'
            r'(?P<crnti>' + str(crnti) + r')\s+'
            r'(?P<cell_id>' + str(cell) + r')\s+'
            r'\[(?P<DL_MCS_0>\d+)\s*/\s*(?P<DL_MCS_0_ACK>\d+)\s*/\s*(?P<DL_MCS_0_NACK>\d+)\s*/\s*(?P<DL_MCS_0_DTX>\d+)\s*/\s*(?P<DL_MCS_0_BLER>\d+\.\d+)\s*\]'
            r'\[(?P<DL_MCS_1>\d+)\s*/\s*(?P<DL_MCS_1_ACK>\d+)\s*/\s*(?P<DL_MCS_1_NACK>\d+)\s*/\s*(?P<DL_MCS_1_DTX>\d+)\s*/\s*(?P<DL_MCS_1_BLER>\d+\.\d+)\s*\]'
            r'\[(?P<DL_MCS_2>\d+)\s*/\s*(?P<DL_MCS_2_ACK>\d+)\s*/\s*(?P<DL_MCS_2_NACK>\d+)\s*/\s*(?P<DL_MCS_2_DTX>\d+)\s*/\s*(?P<DL_MCS_2_BLER>\d+\.\d+)\s*\]'
            r'\[(?P<DL_MCS_3>\d+)\s*/\s*(?P<DL_MCS_3_ACK>\d+)\s*/\s*(?P<DL_MCS_3_NACK>\d+)\s*/\s*(?P<DL_MCS_3_DTX>\d+)\s*/\s*(?P<DL_MCS_3_BLER>\d+\.\d+)\s*\]'
            r'\[(?P<DL_MCS_4>\d+)\s*/\s*(?P<DL_MCS_4_ACK>\d+)\s*/\s*(?P<DL_MCS_4_NACK>\d+)\s*/\s*(?P<DL_MCS_4_DTX>\d+)\s*/\s*(?P<DL_MCS_4_BLER>\d+\.\d+)\s*\]'
            r'\[(?P<DL_MCS_5>\d+)\s*/\s*(?P<DL_MCS_5_ACK>\d+)\s*/\s*(?P<DL_MCS_5_NACK>\d+)\s*/\s*(?P<DL_MCS_5_DTX>\d+)\s*/\s*(?P<DL_MCS_5_BLER>\d+\.\d+)\s*\]'
            r'\[(?P<DL_MCS_6>\d+)\s*/\s*(?P<DL_MCS_6_ACK>\d+)\s*/\s*(?P<DL_MCS_6_NACK>\d+)\s*/\s*(?P<DL_MCS_6_DTX>\d+)\s*/\s*(?P<DL_MCS_6_BLER>\d+\.\d+)\s*\]'
            r'\[(?P<DL_MCS_7>\d+)\s*/\s*(?P<DL_MCS_7_ACK>\d+)\s*/\s*(?P<DL_MCS_7_NACK>\d+)\s*/\s*(?P<DL_MCS_7_DTX>\d+)\s*/\s*(?P<DL_MCS_7_BLER>\d+\.\d+)\s*\]'
            r'\[(?P<DL_MCS_8>\d+)\s*/\s*(?P<DL_MCS_8_ACK>\d+)\s*/\s*(?P<DL_MCS_8_NACK>\d+)\s*/\s*(?P<DL_MCS_8_DTX>\d+)\s*/\s*(?P<DL_MCS_8_BLER>\d+\.\d+)\s*\]'
            r'\[(?P<DL_MCS_9>\d+)\s*/\s*(?P<DL_MCS_9_ACK>\d+)\s*/\s*(?P<DL_MCS_9_NACK>\d+)\s*/\s*(?P<DL_MCS_9_DTX>\d+)\s*/\s*(?P<DL_MCS_9_BLER>\d+\.\d+)\s*\]'
            r'\[(?P<DL_MCS_10>\d+)\s*/\s*(?P<DL_MCS_10_ACK>\d+)\s*/\s*(?P<DL_MCS_10_NACK>\d+)\s*/\s*(?P<DL_MCS_10_DTX>\d+)\s*/\s*(?P<DL_MCS_10_BLER>\d+\.\d+)\s*\]'
            r'\[(?P<DL_MCS_11>\d+)\s*/\s*(?P<DL_MCS_11_ACK>\d+)\s*/\s*(?P<DL_MCS_11_NACK>\d+)\s*/\s*(?P<DL_MCS_11_DTX>\d+)\s*/\s*(?P<DL_MCS_11_BLER>\d+\.\d+)\s*\]'
            r'\[(?P<DL_MCS_12>\d+)\s*/\s*(?P<DL_MCS_12_ACK>\d+)\s*/\s*(?P<DL_MCS_12_NACK>\d+)\s*/\s*(?P<DL_MCS_12_DTX>\d+)\s*/\s*(?P<DL_MCS_12_BLER>\d+\.\d+)\s*\]'
            r'\[(?P<DL_MCS_13>\d+)\s*/\s*(?P<DL_MCS_13_ACK>\d+)\s*/\s*(?P<DL_MCS_13_NACK>\d+)\s*/\s*(?P<DL_MCS_13_DTX>\d+)\s*/\s*(?P<DL_MCS_13_BLER>\d+\.\d+)\s*\]'
            r'\[(?P<DL_MCS_14>\d+)\s*/\s*(?P<DL_MCS_14_ACK>\d+)\s*/\s*(?P<DL_MCS_14_NACK>\d+)\s*/\s*(?P<DL_MCS_14_DTX>\d+)\s*/\s*(?P<DL_MCS_14_BLER>\d+\.\d+)\s*\]'
            r'\[(?P<DL_MCS_15>\d+)\s*/\s*(?P<DL_MCS_15_ACK>\d+)\s*/\s*(?P<DL_MCS_15_NACK>\d+)\s*/\s*(?P<DL_MCS_15_DTX>\d+)\s*/\s*(?P<DL_MCS_15_BLER>\d+\.\d+)\s*\]'
            r'\[(?P<DL_MCS_16>\d+)\s*/\s*(?P<DL_MCS_16_ACK>\d+)\s*/\s*(?P<DL_MCS_16_NACK>\d+)\s*/\s*(?P<DL_MCS_16_DTX>\d+)\s*/\s*(?P<DL_MCS_16_BLER>\d+\.\d+)\s*\]'
            r'\[(?P<DL_MCS_17>\d+)\s*/\s*(?P<DL_MCS_17_ACK>\d+)\s*/\s*(?P<DL_MCS_17_NACK>\d+)\s*/\s*(?P<DL_MCS_17_DTX>\d+)\s*/\s*(?P<DL_MCS_17_BLER>\d+\.\d+)\s*\]'
            r'\[(?P<DL_MCS_18>\d+)\s*/\s*(?P<DL_MCS_18_ACK>\d+)\s*/\s*(?P<DL_MCS_18_NACK>\d+)\s*/\s*(?P<DL_MCS_18_DTX>\d+)\s*/\s*(?P<DL_MCS_18_BLER>\d+\.\d+)\s*\]'
            r'\[(?P<DL_MCS_19>\d+)\s*/\s*(?P<DL_MCS_19_ACK>\d+)\s*/\s*(?P<DL_MCS_19_NACK>\d+)\s*/\s*(?P<DL_MCS_19_DTX>\d+)\s*/\s*(?P<DL_MCS_19_BLER>\d+\.\d+)\s*\]'
            r'\[(?P<DL_MCS_20>\d+)\s*/\s*(?P<DL_MCS_20_ACK>\d+)\s*/\s*(?P<DL_MCS_20_NACK>\d+)\s*/\s*(?P<DL_MCS_20_DTX>\d+)\s*/\s*(?P<DL_MCS_20_BLER>\d+\.\d+)\s*\]'
            r'\[(?P<DL_MCS_21>\d+)\s*/\s*(?P<DL_MCS_21_ACK>\d+)\s*/\s*(?P<DL_MCS_21_NACK>\d+)\s*/\s*(?P<DL_MCS_21_DTX>\d+)\s*/\s*(?P<DL_MCS_21_BLER>\d+\.\d+)\s*\]'
            r'\[(?P<DL_MCS_22>\d+)\s*/\s*(?P<DL_MCS_22_ACK>\d+)\s*/\s*(?P<DL_MCS_22_NACK>\d+)\s*/\s*(?P<DL_MCS_22_DTX>\d+)\s*/\s*(?P<DL_MCS_22_BLER>\d+\.\d+)\s*\]'
            r'\[(?P<DL_MCS_23>\d+)\s*/\s*(?P<DL_MCS_23_ACK>\d+)\s*/\s*(?P<DL_MCS_23_NACK>\d+)\s*/\s*(?P<DL_MCS_23_DTX>\d+)\s*/\s*(?P<DL_MCS_23_BLER>\d+\.\d+)\s*\]'
            r'\[(?P<DL_MCS_24>\d+)\s*/\s*(?P<DL_MCS_24_ACK>\d+)\s*/\s*(?P<DL_MCS_24_NACK>\d+)\s*/\s*(?P<DL_MCS_24_DTX>\d+)\s*/\s*(?P<DL_MCS_24_BLER>\d+\.\d+)\s*\]'
            r'\[(?P<DL_MCS_25>\d+)\s*/\s*(?P<DL_MCS_25_ACK>\d+)\s*/\s*(?P<DL_MCS_25_NACK>\d+)\s*/\s*(?P<DL_MCS_25_DTX>\d+)\s*/\s*(?P<DL_MCS_25_BLER>\d+\.\d+)\s*\]'
            r'\[(?P<DL_MCS_26>\d+)\s*/\s*(?P<DL_MCS_26_ACK>\d+)\s*/\s*(?P<DL_MCS_26_NACK>\d+)\s*/\s*(?P<DL_MCS_26_DTX>\d+)\s*/\s*(?P<DL_MCS_26_BLER>\d+\.\d+)\s*\]'
            r'\[(?P<DL_MCS_27>\d+)\s*/\s*(?P<DL_MCS_27_ACK>\d+)\s*/\s*(?P<DL_MCS_27_NACK>\d+)\s*/\s*(?P<DL_MCS_27_DTX>\d+)\s*/\s*(?P<DL_MCS_27_BLER>\d+\.\d+)\s*\]'
            r'\[(?P<DL_MCS_28>\d+)\s*/\s*(?P<DL_MCS_28_ACK>\d+)\s*/\s*(?P<DL_MCS_28_NACK>\d+)\s*/\s*(?P<DL_MCS_28_DTX>\d+)\s*/\s*(?P<DL_MCS_28_BLER>\d+\.\d+)\s*\]'
            r'\[(?P<DL_MCS_29>\d+)\s*/\s*(?P<DL_MCS_29_ACK>\d+)\s*/\s*(?P<DL_MCS_29_NACK>\d+)\s*/\s*(?P<DL_MCS_29_DTX>\d+)\s*/\s*(?P<DL_MCS_29_BLER>\d+\.\d+)\s*\]'
            r'\[(?P<DL_MCS_30>\d+)\s*/\s*(?P<DL_MCS_30_ACK>\d+)\s*/\s*(?P<DL_MCS_30_NACK>\d+)\s*/\s*(?P<DL_MCS_30_DTX>\d+)\s*/\s*(?P<DL_MCS_30_BLER>\d+\.\d+)\s*\]'
            r'\[(?P<DL_MCS_31>\d+)\s*/\s*(?P<DL_MCS_31_ACK>\d+)\s*/\s*(?P<DL_MCS_31_NACK>\d+)\s*/\s*(?P<DL_MCS_31_DTX>\d+)\s*/\s*(?P<DL_MCS_31_BLER>\d+\.\d+)\s*\]',
            re.MULTILINE
        )
       
        dtype = [('crnti', np.int64), ('cell_id', np.int64)]
        for i in range(32):
            dtype.extend([
                (f'DL_MCS_{i}', np.int64),
                (f'DL_ACK_{i}', np.int64),
                (f'DL_NACK_{i}', np.int64),
                (f'DL_DTX_{i}', np.int64),
                (f'DL_BLER_{i}', np.float64)
            ])
        return np.fromregex(filename, reg, dtype)
    
    def GetUlMcsHistStat(self, filename, cell, crnti):
        reg = re.compile(
            r'UE SCH:UL MCS Histogram Statistics\s+'
            r'[-]+\s+'
            r'UE-ID\s+CELL-ID\s+\[UL-MCS-0.*\]\[UL-MCS-1.*\].*\s+'
            r'(?P<crnti>' + str(crnti) + r')\s+'
            r'(?P<cell_id>' + str(cell) + r')\s+'
            r'\[(?P<UL_MCS_0>\d+)\s*/\s*(?P<UL_MCS_0_ACK>\d+)\s*/\s*(?P<UL_MCS_0_NACK>\d+)\s*/\s*(?P<UL_MCS_0_BLER>\d+\.\d+)\s*\]'
            r'\[(?P<UL_MCS_1>\d+)\s*/\s*(?P<UL_MCS_1_ACK>\d+)\s*/\s*(?P<UL_MCS_1_NACK>\d+)\s*/\s*(?P<UL_MCS_1_BLER>\d+\.\d+)\s*\]'
            r'\[(?P<UL_MCS_2>\d+)\s*/\s*(?P<UL_MCS_2_ACK>\d+)\s*/\s*(?P<UL_MCS_2_NACK>\d+)\s*/\s*(?P<UL_MCS_2_BLER>\d+\.\d+)\s*\]'
            r'\[(?P<UL_MCS_3>\d+)\s*/\s*(?P<UL_MCS_3_ACK>\d+)\s*/\s*(?P<UL_MCS_3_NACK>\d+)\s*/\s*(?P<UL_MCS_3_BLER>\d+\.\d+)\s*\]'
            r'\[(?P<UL_MCS_4>\d+)\s*/\s*(?P<UL_MCS_4_ACK>\d+)\s*/\s*(?P<UL_MCS_4_NACK>\d+)\s*/\s*(?P<UL_MCS_4_BLER>\d+\.\d+)\s*\]'
            r'\[(?P<UL_MCS_5>\d+)\s*/\s*(?P<UL_MCS_5_ACK>\d+)\s*/\s*(?P<UL_MCS_5_NACK>\d+)\s*/\s*(?P<UL_MCS_5_BLER>\d+\.\d+)\s*\]'
            r'\[(?P<UL_MCS_6>\d+)\s*/\s*(?P<UL_MCS_6_ACK>\d+)\s*/\s*(?P<UL_MCS_6_NACK>\d+)\s*/\s*(?P<UL_MCS_6_BLER>\d+\.\d+)\s*\]'
            r'\[(?P<UL_MCS_7>\d+)\s*/\s*(?P<UL_MCS_7_ACK>\d+)\s*/\s*(?P<UL_MCS_7_NACK>\d+)\s*/\s*(?P<UL_MCS_7_BLER>\d+\.\d+)\s*\]'
            r'\[(?P<UL_MCS_8>\d+)\s*/\s*(?P<UL_MCS_8_ACK>\d+)\s*/\s*(?P<UL_MCS_8_NACK>\d+)\s*/\s*(?P<UL_MCS_8_BLER>\d+\.\d+)\s*\]'
            r'\[(?P<UL_MCS_9>\d+)\s*/\s*(?P<UL_MCS_9_ACK>\d+)\s*/\s*(?P<UL_MCS_9_NACK>\d+)\s*/\s*(?P<UL_MCS_9_BLER>\d+\.\d+)\s*\]'
            r'\[(?P<UL_MCS_10>\d+)\s*/\s*(?P<UL_MCS_10_ACK>\d+)\s*/\s*(?P<UL_MCS_10_NACK>\d+)\s*/\s*(?P<UL_MCS_10_BLER>\d+\.\d+)\s*\]'
            r'\[(?P<UL_MCS_11>\d+)\s*/\s*(?P<UL_MCS_11_ACK>\d+)\s*/\s*(?P<UL_MCS_11_NACK>\d+)\s*/\s*(?P<UL_MCS_11_BLER>\d+\.\d+)\s*\]'
            r'\[(?P<UL_MCS_12>\d+)\s*/\s*(?P<UL_MCS_12_ACK>\d+)\s*/\s*(?P<UL_MCS_12_NACK>\d+)\s*/\s*(?P<UL_MCS_12_BLER>\d+\.\d+)\s*\]'
            r'\[(?P<UL_MCS_13>\d+)\s*/\s*(?P<UL_MCS_13_ACK>\d+)\s*/\s*(?P<UL_MCS_13_NACK>\d+)\s*/\s*(?P<UL_MCS_13_BLER>\d+\.\d+)\s*\]'
            r'\[(?P<UL_MCS_14>\d+)\s*/\s*(?P<UL_MCS_14_ACK>\d+)\s*/\s*(?P<UL_MCS_14_NACK>\d+)\s*/\s*(?P<UL_MCS_14_BLER>\d+\.\d+)\s*\]'
            r'\[(?P<UL_MCS_15>\d+)\s*/\s*(?P<UL_MCS_15_ACK>\d+)\s*/\s*(?P<UL_MCS_15_NACK>\d+)\s*/\s*(?P<UL_MCS_15_BLER>\d+\.\d+)\s*\]'
            r'\[(?P<UL_MCS_16>\d+)\s*/\s*(?P<UL_MCS_16_ACK>\d+)\s*/\s*(?P<UL_MCS_16_NACK>\d+)\s*/\s*(?P<UL_MCS_16_BLER>\d+\.\d+)\s*\]'
            r'\[(?P<UL_MCS_17>\d+)\s*/\s*(?P<UL_MCS_17_ACK>\d+)\s*/\s*(?P<UL_MCS_17_NACK>\d+)\s*/\s*(?P<UL_MCS_17_BLER>\d+\.\d+)\s*\]'
            r'\[(?P<UL_MCS_18>\d+)\s*/\s*(?P<UL_MCS_18_ACK>\d+)\s*/\s*(?P<UL_MCS_18_NACK>\d+)\s*/\s*(?P<UL_MCS_18_BLER>\d+\.\d+)\s*\]'
            r'\[(?P<UL_MCS_19>\d+)\s*/\s*(?P<UL_MCS_19_ACK>\d+)\s*/\s*(?P<UL_MCS_19_NACK>\d+)\s*/\s*(?P<UL_MCS_19_BLER>\d+\.\d+)\s*\]'
            r'\[(?P<UL_MCS_20>\d+)\s*/\s*(?P<UL_MCS_20_ACK>\d+)\s*/\s*(?P<UL_MCS_20_NACK>\d+)\s*/\s*(?P<UL_MCS_20_BLER>\d+\.\d+)\s*\]'
            r'\[(?P<UL_MCS_21>\d+)\s*/\s*(?P<UL_MCS_21_ACK>\d+)\s*/\s*(?P<UL_MCS_21_NACK>\d+)\s*/\s*(?P<UL_MCS_21_BLER>\d+\.\d+)\s*\]'
            r'\[(?P<UL_MCS_22>\d+)\s*/\s*(?P<UL_MCS_22_ACK>\d+)\s*/\s*(?P<UL_MCS_22_NACK>\d+)\s*/\s*(?P<UL_MCS_22_BLER>\d+\.\d+)\s*\]'
            r'\[(?P<UL_MCS_23>\d+)\s*/\s*(?P<UL_MCS_23_ACK>\d+)\s*/\s*(?P<UL_MCS_23_NACK>\d+)\s*/\s*(?P<UL_MCS_23_BLER>\d+\.\d+)\s*\]'
            r'\[(?P<UL_MCS_24>\d+)\s*/\s*(?P<UL_MCS_24_ACK>\d+)\s*/\s*(?P<UL_MCS_24_NACK>\d+)\s*/\s*(?P<UL_MCS_24_BLER>\d+\.\d+)\s*\]'
            r'\[(?P<UL_MCS_25>\d+)\s*/\s*(?P<UL_MCS_25_ACK>\d+)\s*/\s*(?P<UL_MCS_25_NACK>\d+)\s*/\s*(?P<UL_MCS_25_BLER>\d+\.\d+)\s*\]'
            r'\[(?P<UL_MCS_26>\d+)\s*/\s*(?P<UL_MCS_26_ACK>\d+)\s*/\s*(?P<UL_MCS_26_NACK>\d+)\s*/\s*(?P<UL_MCS_26_BLER>\d+\.\d+)\s*\]'
            r'\[(?P<UL_MCS_27>\d+)\s*/\s*(?P<UL_MCS_27_ACK>\d+)\s*/\s*(?P<UL_MCS_27_NACK>\d+)\s*/\s*(?P<UL_MCS_27_BLER>\d+\.\d+)\s*\]'
            r'\[(?P<UL_MCS_28>\d+)\s*/\s*(?P<UL_MCS_28_ACK>\d+)\s*/\s*(?P<UL_MCS_28_NACK>\d+)\s*/\s*(?P<UL_MCS_28_BLER>\d+\.\d+)\s*\]'
            r'\[(?P<UL_MCS_29>\d+)\s*/\s*(?P<UL_MCS_29_ACK>\d+)\s*/\s*(?P<UL_MCS_29_NACK>\d+)\s*/\s*(?P<UL_MCS_29_BLER>\d+\.\d+)\s*\]'
            r'\[(?P<UL_MCS_30>\d+)\s*/\s*(?P<UL_MCS_30_ACK>\d+)\s*/\s*(?P<UL_MCS_30_NACK>\d+)\s*/\s*(?P<UL_MCS_30_BLER>\d+\.\d+)\s*\]'
            r'\[(?P<UL_MCS_31>\d+)\s*/\s*(?P<UL_MCS_31_ACK>\d+)\s*/\s*(?P<UL_MCS_31_NACK>\d+)\s*/\s*(?P<UL_MCS_31_BLER>\d+\.\d+)\s*\]',
            re.MULTILINE
        )
        dtype = [('crnti', np.int64), ('cell_id', np.int64)]
        for i in range(32):
            dtype.extend([
                (f'UL_MCS_{i}', np.int64),
                (f'UL_MCS_{i}_ACK', np.int64),
                (f'UL_MCS_{i}_NACK', np.int64),
                (f'UL_MCS_{i}_BLER', np.float64)
            ])
        return np.fromregex(filename, reg, dtype)
    
    def GetUeAlgoHistStat(self, filename, cell, crnti):
        reg = re.compile(
            r'UE ALGO:Algo Histogram Statistics\s+'
            r'[-]+\s+'
            r'UE-ID\s+CELL-ID\s+MIN-CB-ALGO-TIME\s+MAX-CB-ALGO-TIME\s+AVG-CB-ALGO-TIME\s+UL-RI-1\s+UL-RI-2\s+UL-RI-3\s+UL-RI-4\s+UL-PMI-0\s+UL-PMI-1\s+UL-PMI-2\s+UL-PMI-3\s+UL-PMI-4\s+UL-PMI-5\s+WSINR=-10\s+WSINR<-5\s+WSINR<0\s+WSINR<5\s+WSINR<10\s+WSINR<15\s+WSINR<20\s+WSINR<25\s+WSINR<30\s+WSINR<35\s+WSINR<40\s+WSINR=40\s+'
            r'(?P<crnti>' + str(crnti) + r')\s+'
            r'(?P<cell_id>' + str(cell) + r')\s+'
            r'(?P<MIN_CB_ALGO_TIME>\d+)\s+'
            r'(?P<MAX_CB_ALGO_TIME>\d+)\s+'
            r'(?P<AVG_CB_ALGO_TIME>\d+(?:\.\d+)?)\s+'
            r'(?P<UL_RI_1>\d+)\s+'
            r'(?P<UL_RI_2>\d+)\s+'
            r'(?P<UL_RI_3>\d+)\s+'
            r'(?P<UL_RI_4>\d+)\s+'
            r'(?P<UL_PMI_0>\d+)\s+'
            r'(?P<UL_PMI_1>\d+)\s+'
            r'(?P<UL_PMI_2>\d+)\s+'
            r'(?P<UL_PMI_3>\d+)\s+'
            r'(?P<UL_PMI_4>\d+)\s+'
            r'(?P<UL_PMI_5>\d+)\s+'
            r'(?P<WSINR_NEG10>\d+)\s+'
            r'(?P<WSINR_LT_NEG5>\d+)\s+'
            r'(?P<WSINR_LT0>\d+)\s+'
            r'(?P<WSINR_LT5>\d+)\s+'
            r'(?P<WSINR_LT10>\d+)\s+'
            r'(?P<WSINR_LT15>\d+)\s+'
            r'(?P<WSINR_LT20>\d+)\s+'
            r'(?P<WSINR_LT25>\d+)\s+'
            r'(?P<WSINR_LT30>\d+)\s+'
            r'(?P<WSINR_LT35>\d+)\s+'
            r'(?P<WSINR_LT40>\d+)\s+'
            r'(?P<WSINR_40>\d+)',
            re.MULTILINE
        )
        print("Getting UE ALGO Histogram Stats")
        
        result = np.fromregex(filename, reg, [
            ('crnti', np.int64), 
            ('cell_id', np.int64),
            ('MIN_CB_ALGO_TIME', np.int64),
            ('MAX_CB_ALGO_TIME', np.int64),
            ('AVG_CB_ALGO_TIME', np.float64),
            ('UL_RI_1', np.int64),
            ('UL_RI_2', np.int64),
            ('UL_RI_3', np.int64),
            ('UL_RI_4', np.int64),
            ('UL_PMI_0', np.int64),
            ('UL_PMI_1', np.int64),
            ('UL_PMI_2', np.int64),
            ('UL_PMI_3', np.int64),
            ('UL_PMI_4', np.int64),
            ('UL_PMI_5', np.int64),
            ('WSINR_NEG10', np.int64),
            ('WSINR_LT_NEG5', np.int64),
            ('WSINR_LT0', np.int64),
            ('WSINR_LT5', np.int64),
            ('WSINR_LT10', np.int64),
            ('WSINR_LT15', np.int64),
            ('WSINR_LT20', np.int64),
            ('WSINR_LT25', np.int64),
            ('WSINR_LT30', np.int64),
            ('WSINR_LT35', np.int64),
            ('WSINR_LT40', np.int64),
            ('WSINR_40', np.int64)
        ])
    
    def GetUeAlgoPuschSinrHistStat(self, filename, cell, crnti):
        reg = re.compile(
            r'UE SCH: UL SNR Histogram Statistics\s+'
            r'[-]+\s+'
            r'UE-ID\s+CELL-ID\s+PUSCH-SNR<=-10\s+-9:-6\s+-5:-2\s+-1:2\s+3:6\s+7:10\s+11:14\s+15:18\s+19:22\s+23:26\s+27:30\s+>30\s+SRS-SNR<=-10\s+-9:-6\s+-5:-2\s+-1:2\s+3:6\s+7:10\s+11:14\s+15:18\s+19:22\s+23:26\s+27:30\s+>30\s+'
            r'(?P<crnti>' + str(crnti) + r')\s+'
            r'(?P<cell_id>' + str(cell) + r')\s+'
            r'(?P<PUSCH_SNR_LTE_10>\d+)\s+'
            r'(?P<PUSCH_SNR_9_6>\d+)\s+'
            r'(?P<PUSCH_SNR_5_2>\d+)\s+'
            r'(?P<PUSCH_SNR_1_2>\d+)\s+'
            r'(?P<PUSCH_SNR_3_6>\d+)\s+'
            r'(?P<PUSCH_SNR_7_10>\d+)\s+'
            r'(?P<PUSCH_SNR_11_14>\d+)\s+'
            r'(?P<PUSCH_SNR_15_18>\d+)\s+'
            r'(?P<PUSCH_SNR_19_22>\d+)\s+'
            r'(?P<PUSCH_SNR_23_26>\d+)\s+'
            r'(?P<PUSCH_SNR_27_30>\d+)\s+'
            r'(?P<PUSCH_SNR_GT_30>\d+)\s+'
            r'(?P<SRS_SNR_LTE_10>\d+)\s+'
            r'(?P<SRS_SNR_9_6>\d+)\s+'
            r'(?P<SRS_SNR_5_2>\d+)\s+'
            r'(?P<SRS_SNR_1_2>\d+)\s+'
            r'(?P<SRS_SNR_3_6>\d+)\s+'
            r'(?P<SRS_SNR_7_10>\d+)\s+'
            r'(?P<SRS_SNR_11_14>\d+)\s+'
            r'(?P<SRS_SNR_15_18>\d+)\s+'
            r'(?P<SRS_SNR_19_22>\d+)\s+'
            r'(?P<SRS_SNR_23_26>\d+)\s+'
            r'(?P<SRS_SNR_27_30>\d+)\s+'
            r'(?P<SRS_SNR_GT_30>\d+)',
            re.MULTILINE
        )
        
        print("Getting UL SNR Histogram Stats")
        
        return np.fromregex(filename, reg, [
            ('crnti', np.int64), 
            ('cell_id', np.int64),
            ('PUSCH_SNR_LTE_10', np.int64),
            ('PUSCH_SNR_9_6', np.int64),
            ('PUSCH_SNR_5_2', np.int64),
            ('PUSCH_SNR_1_2', np.int64),
            ('PUSCH_SNR_3_6', np.int64),
            ('PUSCH_SNR_7_10', np.int64),
            ('PUSCH_SNR_11_14', np.int64),
            ('PUSCH_SNR_15_18', np.int64),
            ('PUSCH_SNR_19_22', np.int64),
            ('PUSCH_SNR_23_26', np.int64),
            ('PUSCH_SNR_27_30', np.int64),
            ('PUSCH_SNR_GT_30', np.int64),
            ('SRS_SNR_LTE_10', np.int64),
            ('SRS_SNR_9_6', np.int64),
            ('SRS_SNR_5_2', np.int64),
            ('SRS_SNR_1_2', np.int64),
            ('SRS_SNR_3_6', np.int64),
            ('SRS_SNR_7_10', np.int64),
            ('SRS_SNR_11_14', np.int64),
            ('SRS_SNR_15_18', np.int64),
            ('SRS_SNR_19_22', np.int64),
            ('SRS_SNR_23_26', np.int64),
            ('SRS_SNR_27_30', np.int64),
            ('SRS_SNR_GT_30', np.int64)
        ])
    
    def GetUeUlPowerCtrlPucchPuschStat(self, filename, cell, crnti):
        reg = re.compile(
            r'UE SCH:UL POWER CONTROL PUCCH/PUSCH Instantaneous Statistics\s+'
            r'[-]+\s+'
            r'UE-ID\s+CELL-ID\s+PUSCH-SNR\s+PHR\s+PH-VAL\s+ACC_TPC\s+PATHLOSS\s+PUSCH-TPC\s+PUSCH-TPC\[0\]\s+PUSCH-TPC\[1\]\s+PUSCH-TPC\[2\]\s+PUSCH-TPC\[3\]\s+UL-PRB-REQ\s+UL-PWR-PRB\s+PUCCH-SNR\s+PWR-DELTA\s+PUCCH-TPC\s+PUCCH-TPC\[0\]\s+PUCCH-TPC\[1\]\s+PUCCH-TPC\[2\]\s+PUCCH-TPC\[3\]\s+'
            r'(?P<crnti>' + str(crnti) + r')\s+'
            r'(?P<cell_id>' + str(cell) + r')\s+'
            r'(?P<PUSCH_SNR>-?\d+)\s+'
            r'(?P<PHR>\d+)\s+'
            r'(?P<PH_VAL>\d+)\s+'
            r'(?P<ACC_TPC>\d+)\s+'
            r'(?P<PATHLOSS>\d+)\s+'
            r'(?P<PUSCH_TPC>\d+)\s+'
            r'(?P<PUSCH_TPC_0>\d+)\s+'
            r'(?P<PUSCH_TPC_1>\d+)\s+'
            r'(?P<PUSCH_TPC_2>\d+)\s+'
            r'(?P<PUSCH_TPC_3>\d+)\s+'
            r'(?P<UL_PRB_REQ>\d+)\s+'
            r'(?P<UL_PWR_PRB>\d+)\s+'
            r'(?P<PUCCH_SNR>-?\d+)\s+'
            r'(?P<PWR_DELTA>\d+)\s+'
            r'(?P<PUCCH_TPC>\d+)\s+'
            r'(?P<PUCCH_TPC_0>\d+)\s+'
            r'(?P<PUCCH_TPC_1>\d+)\s+'
            r'(?P<PUCCH_TPC_2>\d+)\s+'
            r'(?P<PUCCH_TPC_3>\d+)',
            re.MULTILINE
        )
        
        print("Getting UL POWER CONTROL PUCCH/PUSCH Stats")
        
        return np.fromregex(filename, reg, [
            ('crnti', np.int64), 
            ('cell_id', np.int64),
            ('PUSCH_SNR', np.int64),
            ('PHR', np.int64),
            ('PH_VAL', np.int64),
            ('ACC_TPC', np.int64),
            ('PATHLOSS', np.int64),
            ('PUSCH_TPC', np.int64),
            ('PUSCH_TPC_0', np.int64),
            ('PUSCH_TPC_1', np.int64),
            ('PUSCH_TPC_2', np.int64),
            ('PUSCH_TPC_3', np.int64),
            ('UL_PRB_REQ', np.int64),
            ('UL_PWR_PRB', np.int64),
            ('PUCCH_SNR', np.int64),
            ('PWR_DELTA', np.int64),
            ('PUCCH_TPC', np.int64),
            ('PUCCH_TPC_0', np.int64),
            ('PUCCH_TPC_1', np.int64),
            ('PUCCH_TPC_2', np.int64),
            ('PUCCH_TPC_3', np.int64)
        ])
    
    def GetUeUciPucchPuschStat(self, filename, cell, crnti):
        reg = re.compile(
            r'UE SCH:UCI on PUCCH/PUSCH Instantaneous Statistics\s+'
            r'[-]+\s+'
            r'UE-ID\s+CELL-ID\s+UC:EXPT\s+UC:ACK\s+UC:NACK\s+UC:DTX\s+UC:F0-DTX\s+UC:F1-DTX\s+UC:F2-DTX\s+UC:F3-DTX\s+UC:F4-DTX\s+UC:CSI\s+US:EXPT\s+US:ACK\s+US:NACK\s+US:DTX\s+US:F0-DTX\s+US:F1-DTX\s+US:F2-DTX\s+US:F3-DTX\s+US:F4-DTX\s+HqBetaOff1\s+US:CSI\s+'
            r'(?P<crnti>' + str(crnti) + r')\s+'
            r'(?P<cell_id>' + str(cell) + r')\s+'
            r'(?P<UC_EXPT>\d+)\s+'
            r'(?P<UC_ACK>\d+)\s+'
            r'(?P<UC_NACK>\d+)\s+'
            r'(?P<UC_DTX>\d+)\s+'
            r'(?P<UC_F0_DTX>\d+)\s+'
            r'(?P<UC_F1_DTX>\d+)\s+'
            r'(?P<UC_F2_DTX>\d+)\s+'
            r'(?P<UC_F3_DTX>\d+)\s+'
            r'(?P<UC_F4_DTX>\d+)\s+'
            r'(?P<UC_CSI>\d+)\s+'
            r'(?P<US_EXPT>\d+)\s+'
            r'(?P<US_ACK>\d+)\s+'
            r'(?P<US_NACK>\d+)\s+'
            r'(?P<US_DTX>\d+)\s+'
            r'(?P<US_F0_DTX>\d+)\s+'
            r'(?P<US_F1_DTX>\d+)\s+'
            r'(?P<US_F2_DTX>\d+)\s+'
            r'(?P<US_F3_DTX>\d+)\s+'
            r'(?P<US_F4_DTX>\d+)\s+'
            r'(?P<HqBetaOff1>\d+)\s+'
            r'(?P<US_CSI>\d+)',
            re.MULTILINE
        )
        
        print("Getting UCI on PUCCH/PUSCH Stats")
        
        return np.fromregex(filename, reg, [
            ('crnti', np.int64), 
            ('cell_id', np.int64),
            ('UC_EXPT', np.int64),
            ('UC_ACK', np.int64),
            ('UC_NACK', np.int64),
            ('UC_DTX', np.int64),
            ('UC_F0_DTX', np.int64),
            ('UC_F1_DTX', np.int64),
            ('UC_F2_DTX', np.int64),
            ('UC_F3_DTX', np.int64),
            ('UC_F4_DTX', np.int64),
            ('UC_CSI', np.int64),
            ('US_EXPT', np.int64),
            ('US_ACK', np.int64),
            ('US_NACK', np.int64),
            ('US_DTX', np.int64),
            ('US_F0_DTX', np.int64),
            ('US_F1_DTX', np.int64),
            ('US_F2_DTX', np.int64),
            ('US_F3_DTX', np.int64),
            ('US_F4_DTX', np.int64),
            ('HqBetaOff1', np.int64),
            ('US_CSI', np.int64)
        ])
    
    def getRachCumlStat(self, filename, cell):
        reg = re.compile(
            r'RACH Cumulative Statistics\s+'
            r'[-]+\s+'
            r'CELL-ID\s+Pmbl-Dctd\s+Pmbl-Igrd\s+Cfra-Pmbl-Dctd\s+Cbra-Pmbl-Dctd\s+Num-RAR\s+Crnti-Not-Avl\s+Msg3-Ded-Succ\s+Msg3-NonDed-Succ\s+Msg3-VeryLowSnr\s+Msg3-Crnti-CE\s+Msg4-Succ\s+ContRes-Tmr-Exp\s+RLS_MSG3_FAIL\s+RLS_T300_EXP\s+GUECB_LMT_HIT_RA_RSP\s+BACK_OFF_IND\s+MSG4_TX_COUNT\s+CCCH_TX_COUNT\s+MSG4_CCCH_TX_COUNT\s+'
            r'(?P<cell_id>' + str(cell) + r')\s+'
            r'(?P<Pmbl_Dctd>\d+)\s+'
            r'(?P<Pmbl_Igrd>\d+)\s+'
            r'(?P<Cfra_Pmbl_Dctd>\d+)\s+'
            r'(?P<Cbra_Pmbl_Dctd>\d+)\s+'
            r'(?P<Num_RAR>\d+)\s+'
            r'(?P<Crnti_Not_Avl>\d+)\s+'
            r'(?P<Msg3_Ded_Succ>\d+)\s+'
            r'(?P<Msg3_NonDed_Succ>\d+)\s+'
            r'(?P<Msg3_VeryLowSnr>\d+)\s+'
            r'(?P<Msg3_Crnti_CE>\d+)\s+'
            r'(?P<Msg4_Succ>\d+)\s+'
            r'(?P<ContRes_Tmr_Exp>\d+)\s+'
            r'(?P<RLS_MSG3_FAIL>\d+)\s+'
            r'(?P<RLS_T300_EXP>\d+)\s+'
            r'(?P<GUECB_LMT_HIT_RA_RSP>\d+)\s+'
            r'(?P<BACK_OFF_IND>\d+)\s+'
            r'(?P<MSG4_TX_COUNT>\d+)\s+'
            r'(?P<CCCH_TX_COUNT>\d+)\s+'
            r'(?P<MSG4_CCCH_TX_COUNT>\d+)',
            re.MULTILINE
        )
        
        print("Getting RACH Cumulative Stats")
        
        return np.fromregex(filename, reg, [
            ('cell_id', np.int64),
            ('Pmbl_Dctd', np.int64),
            ('Pmbl_Igrd', np.int64),
            ('Cfra_Pmbl_Dctd', np.int64),
            ('Cbra_Pmbl_Dctd', np.int64),
            ('Num_RAR', np.int64),
            ('Crnti_Not_Avl', np.int64),
            ('Msg3_Ded_Succ', np.int64),
            ('Msg3_NonDed_Succ', np.int64),
            ('Msg3_VeryLowSnr', np.int64),
            ('Msg3_Crnti_CE', np.int64),
            ('Msg4_Succ', np.int64),
            ('ContRes_Tmr_Exp', np.int64),
            ('RLS_MSG3_FAIL', np.int64),
            ('RLS_T300_EXP', np.int64),
            ('GUECB_LMT_HIT_RA_RSP', np.int64),
            ('BACK_OFF_IND', np.int64),
            ('MSG4_TX_COUNT', np.int64),
            ('CCCH_TX_COUNT', np.int64),
            ('MSG4_CCCH_TX_COUNT', np.int64)
        ])
    
    # def get_csv(self):
    #     data = []
    #     sys.stdout.write(f'started processing du stats from path {self.path}\n')
    #     for file in glob.iglob(f'{self.path}/*.txt*'):
    #         sys.stdout.write('processing file ' + os.path.basename(file) + '\n')
    #         for cell in range(1, self.num_cc + 1):
    #             data.append(pd.concat([pd.DataFrame(self.get_time_stamp(file)),              pd.DataFrame(self.get_blr(file, cell)), pd.DataFrame(self.get_thpt(file, cell)), pd.DataFrame(self.get_cqi_ri_histogram(file, cell))], axis = 1))
    #     data = pd.concat(data)
    #     name = os.path.basename(file).rsplit('_part', 1)
    #     return data.to_csv(self.setup + '_' + name[0] + '.csv', index=False) if self.csv_out_path == False else data.to_csv(self.csv_out_path +  self.setup + '_' + name[0] + '.csv', index=False), sys.stdout.write(f'processed output file at {os.path.dirname(os.path.abspath(sys.argv[0]))}\n') if self.csv_out_path == False else sys.stdout.write(f'processed file at {self.csv_out_path}\n') 
    
    def get_xls(self):
        sheet_1 = [] 
        sheet_2 = [] 
        sheet_3 = []
        sheet_4 = []
        sheet_5 = []
        sheet_6 = []
        sheet_7 = []
        sheet_8 = []
        sheet_9 = []
        sheet_10 = []
        sheet_11 = []
        sheet_12 = []
        sheet_13 = []
        file = None
        # crnti = None
        sys.stdout.write(f'started processing du stats from path {self.path}\n')
        for file in glob.iglob(f'{self.path}/*.txt*'):
            sys.stdout.write(f'processing {file} in dir ' + os.path.basename(self.path) + '\n')
            for cell in range(1, self.num_cc + 1):
                sheet_1.append(pd.DataFrame(pd.concat([pd.DataFrame(self.GetTimeStamp(file)), pd.DataFrame(self.GetBlrStats(file, cell))], axis = 1)))
                print("sheet 1")
                sheet_2.append(pd.DataFrame(pd.concat([pd.DataFrame(self.GetTimeStamp(file)), pd.DataFrame(self.GetThptStats(file, cell))], axis = 1)))
                print("sheet 2")
                sheet_3.append(pd.DataFrame(pd.concat([pd.DataFrame(self.GetTimeStamp(file)), pd.DataFrame(self.GetCqiRiHist(file, cell))], axis = 1)))
                print("sheet 3")
                sheet_4.append(pd.DataFrame(pd.concat([pd.DataFrame(self.GetTimeStamp(file)), pd.DataFrame(self.GetCellInstStats(file, cell))], axis = 1)))
                print("sheet 4 completed")
                sheet_5.append(pd.DataFrame(pd.concat([pd.DataFrame(self.GetTimeStamp(file)), pd.DataFrame(self.GetUeDrxInstStat(file, cell, self.crnti))], axis = 1)))
                print("sheet 5")
                sheet_6.append(pd.DataFrame(pd.concat([pd.DataFrame(self.GetTimeStamp(file)), pd.DataFrame(self.GetUeLaHistStat(file, cell, self.crnti))], axis = 1)))
                print("sheet 6")
                sheet_7.append(pd.DataFrame(pd.concat([pd.DataFrame(self.GetTimeStamp(file)), pd.DataFrame(self.GetDlMcsHistStat(file, cell, self.crnti))], axis = 1)))
                print("sheet 7")
                sheet_8.append(pd.DataFrame(pd.concat([pd.DataFrame(self.GetTimeStamp(file)), pd.DataFrame(self.GetUlMcsHistStat(file, cell, self.crnti))], axis = 1)))
                print("Sheet 8 completed")
                sheet_9.append(pd.DataFrame(pd.concat([pd.DataFrame(self.GetTimeStamp(file)), pd.DataFrame(self.GetUeAlgoHistStat(file, cell, self.crnti))], axis = 1)))
                sheet_10.append(pd.DataFrame(pd.concat([pd.DataFrame(self.GetTimeStamp(file)), pd.DataFrame(self.GetUeAlgoPuschSinrHistStat(file, cell, self.crnti))], axis = 1)))
                sheet_11.append(pd.DataFrame(pd.concat([pd.DataFrame(self.GetTimeStamp(file)), pd.DataFrame(self.GetUeUlPowerCtrlPucchPuschStat(file, cell, self.crnti))], axis = 1)))
                sheet_12.append(pd.DataFrame(pd.concat([pd.DataFrame(self.GetTimeStamp(file)), pd.DataFrame(self.GetUeUciPucchPuschStat(file, cell, self.crnti))], axis = 1)))
                sheet_13.append(pd.DataFrame(pd.concat([pd.DataFrame(self.GetTimeStamp(file)), pd.DataFrame(self.getRachCumlStat(file, cell))], axis = 1)))
                print(" Sheet 13 Completed")
        name = os.path.basename(file).rsplit('.txt', 1)
        with pd.ExcelWriter(self.csv_out_path + '/' + self.setup + '_' + name[0] + '.xlsx', engine='xlsxwriter') as writer:
            pd.concat(sheet_1).to_excel(writer, sheet_name="bler_stats", index=False)
            pd.concat(sheet_2).to_excel(writer, sheet_name="thpt_stats", index=False)
            pd.concat(sheet_3).to_excel(writer, sheet_name="cqi_ri_stats", index=False)
            pd.concat(sheet_4).to_excel(writer, sheet_name="Cell_Inst_Stats", index=False)
            pd.concat(sheet_5).to_excel(writer, sheet_name="Ue_Drx_Stats", index=False)
            pd.concat(sheet_6).to_excel(writer, sheet_name="Ue_La_Hist_stats", index=False)
            pd.concat(sheet_7).to_excel(writer, sheet_name="dl_mcs_hist_stat", index=False)
            pd.concat(sheet_8).to_excel(writer, sheet_name="ul_mcs_hist_stat", index=False)
            pd.concat(sheet_9).to_excel(writer, sheet_name="ue_algo_hist_stat", index=False)
            pd.concat(sheet_10).to_excel(writer, sheet_name="ue_algo_pusch_sinr_stat", index=False)
            pd.concat(sheet_11).to_excel(writer, sheet_name="ue_pucch_pusch_pwr_ctrl_stat", index=False)
            pd.concat(sheet_12).to_excel(writer, sheet_name="ue_uci_pucch_pusch_stat", index=False)
            pd.concat(sheet_13).to_excel(writer, sheet_name="rach_cum_stat", index=False)
            # pd.concat(sheet_7).to_excel(writer, sheet_name="pdcp_rx_stats", index=False)
        
'''
main functions initializes the du stat object from du stat class and invokes method get_csv().
'''


if __name__ == ('__main__'):    
    du_parser = DuStatParser()
    du_parser.get_xls()





