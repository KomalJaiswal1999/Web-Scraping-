import test

from pandas.tests.io.excel.test_openpyxl import openpyxl
import pytest
import os
from datetime import date
from datetime import datetime

from mainlink import mainlink
from extract import extractdata
from extracfinal import extracfinal
from productdata import productdata
from data import data


store_list = []

def _main(start, end):
    global dir_path
    global _zip
    global _job_number
    global _cookie


    store_file = "Store_Info.xlsx"
    wb_store = openpyxl.load_workbook(store_file)
    sheet_store = wb_store.active

    for row in sheet_store.iter_rows(values_only=True):
        row_list = [row[0], row[1], row[2]]
        if row_list[0] is None:
            break
        else:
            store_list.append(row_list)


    for x in range(start, end + 1):
        _Zip = str(store_list[x][0])
        _JobNumber = str(store_list[x][1])
        _Cookie = str(store_list[x][2])
        today = date.today()
        _Date = today.strftime("%m/%d/%Y")
        print("Start Scraping with _Zip: ", _Zip, "_JobNumber: ", _JobNumber)

        mainlink.mainlink(_Zip,_JobNumber,_Cookie)
        data.data(_Zip,_JobNumber,_Cookie)
        extractdata.extractdata(_Zip, _JobNumber, _Cookie)
        productdata.productdata(_Zip,_JobNumber,_Cookie)
        extracfinal.extracfinal(_Zip,_JobNumber,_Cookie)

