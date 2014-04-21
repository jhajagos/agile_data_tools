__author__ = 'janos'

from bulk_import_data_from_csv_to_db import generate_schema_from_csv_file

import os
samfiles = ['CBR_1.txt','CBR_2.txt','CBR_3.txt','CBR_4.txt','CBR_5.txt','NOG_1.txt','NOG_2.txt']
samfiles = ['CBR_1.txt']
for samfile in samfiles:
    file_name = os.path.join("E:\\data\\xlaev transciptome\\", samfile)
    table_name = samfile.split(".")[0]
    generate_schema_from_csv_file(file_name,
                              "postgresql+pg8000://janos:8cranb@192.168.93.132/janos",
                                table_name, delimiter="\t", no_header=True)