__author__ = 'janos'

from bulk_import_data_from_csv_to_db import generate_schema_from_csv_file



# generate_schema_from_csv_file("Z:/data/2011 Discharge Detail - Catholic Health Services & Stony Brook/2012 Discharge Detail CHS SBU.csv",
#                               "mysql://root:8cranb@localhost/sparcs_chs", "discharges_2012")


# generate_schema_from_csv_file("Z:/data/2011 Discharge Detail - Catholic Health Services & Stony Brook/2012 Discharge Detail CHS SBU.csv",
#                                "postgresql+pg8000://janos:8cranb@192.168.93.132/janos", "discharges_2012")

# generate_schema_from_csv_file("E:/data/sparcs_master_2012/Hospital_Inpatient_Discharges_SPARCS_De-Identified_2012.csv",
#                                "postgresql+pg8000://janos:8cranb@192.168.93.132/janos", "state_wide_sparcs_2012")

# generate_schema_from_csv_file("Z:\\data\\NYSDOC+Claims+1-1-12+to+1-31-12.csv",
#                               "sqlite:///dcos.db3", "dcos")

#
# generate_schema_from_csv_file("E:\\data\\xlaev transciptome\\proteins.genesym.defs.uniq.tsv",
#                                 "postgresql+pg8000://janos:8cranb@192.168.93.132/janos",
#                                 "genesymsxlaev", delimiter="\t", no_header=True
#                                 )
#
#
# generate_schema_from_csv_file("C:\\Users\\janos\\rna_seq4\\detailed_results\\rna_seq4_NOG_1_alignments.txt",
#                               "postgresql+pg8000://janos:8cranb@192.168.93.132/janos",
#                                 "NOG_alignment", delimiter="\t", no_header=True
#                                 )
#
# generate_schema_from_csv_file("C:\\Users\\janos\\rna_seq4\\detailed_results\\rna_seq4_CBR_1_alignments.txt",
#                               "postgresql+pg8000://janos:8cranb@192.168.93.132/janos",
#                                 "CBR_alignment", delimiter="\t", no_header=True
#                                 )
import os
samfiles = ['CBR_1.txt','CBR_2.txt','CBR_3.txt','CBR_4.txt','CBR_5.txt','NOG_1.txt','NOG_2.txt']
samfiles = ['CBR_1.txt']
for samfile in samfiles:
    file_name = os.path.join("E:\\data\\xlaev transciptome\\", samfile)
    table_name = samfile.split(".")[0]
    generate_schema_from_csv_file(file_name,
                              "postgresql+pg8000://janos:8cranb@192.168.93.132/janos",
                                table_name, delimiter="\t", no_header=True)