import win32com.client
import os
import sys

__author__ = 'jhajagos'


"""
In order for this program to work you need to have the win32 com library installed. You also
need a comparable version of 32 bit Python with a 32 bit Excel or 64 bit Python with a 64 bit Excel.

"""

import sys

def csv_from_excel(xls_file_to_convert):

    excel = win32com.client.Dispatch('Excel.Application')
    file_directory, file_name = os.path.split(xls_file_to_convert)
    name_only, ext = os.path.splitext(file_name)

    new_name = name_only + ".csv"
    out_csv_file = os.path.join(file_directory, new_name)

    if os.path.exists(out_csv_file):
        os.remove(out_csv_file)

    workbook = excel.Workbooks.Open(xls_file_to_convert)
    workbook.SaveAs(out_csv_file, 6) # CSV format
    workbook.Close(False)
    excel.Quit()
    del excel
    return out_csv_file


if __name__ == "__main__":

    if len(sys.argv) == 1:
        print("Usage: python convert_excel_to_csv.py .\\test\\test_workbook_to_convert.xlsx")
    else:
        print("Generating '%s'" % csv_from_excel(os.path.abspath(sys.argv[1])))