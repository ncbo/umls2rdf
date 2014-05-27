#Script utility to replace TTL files in BioPortal
#Useful also for BioPortal appliances

from os import listdir
from os.path import isfile,isdir, join
import pdb
import glob
import pdb
import shutil

REPO = "/srv/ncbo/repository"
OUTPUT = "./output"

ttl_files = glob.glob("%s/*.ttl"%(OUTPUT))

file_map = {}
for ttl in ttl_files:
    acronym = ttl.split("/")[-1][0:-4]
    file_map[acronym] = ttl

for acronym in file_map:
    ttl = file_map[acronym]
    dir_ont = join(REPO,acronym)
    if isdir(dir_ont):
        sub_dirs = glob.glob(join(dir_ont,"*"))
        latest = 0
        latest_subdir = None
        for sub_dir in sub_dirs:
            s = sub_dir.split("/")[-1] 
            try:
                i = int(s)
                if i > latest:
                    latest = i
                    latest_subdir = sub_dir
            except ValueError:
                continue
        print "Latest for " + acronym + " is " + str(latest)
        if latest_subdir:
            if isfile(join(latest_subdir,ttl.split("/")[-1])):
                shutil.copy2(ttl,latest_subdir)
                print "ttl found"
            else:
                print "ttl file not found for " + acronym
    else:
        print "NOT Found " + dir_ont

