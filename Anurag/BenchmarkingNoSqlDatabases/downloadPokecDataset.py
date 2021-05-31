import re
import urllib
from urllib import request
import gzip
import shutil
import os


def downloadDataset():

    print("Downloading pokec dataset started..")
    url1 = "https://snap.stanford.edu/data/soc-pokec-relationships.txt.gz"
    file_name1 = re.split(pattern='/', string=url1)[-1]
    r1 = urllib.request.urlretrieve(url=url1, filename=file_name1)
    txt1 = "soc-pokec-relationships.txt"

    with gzip.open(file_name1, 'rb') as f_in:
        with open(txt1, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)

    os.remove("soc-pokec-relationships.txt.gz")


    url1 = "https://snap.stanford.edu/data/soc-pokec-profiles.txt.gz"
    file_name1 = re.split(pattern='/', string=url1)[-1]
    r1 = urllib.request.urlretrieve(url=url1, filename=file_name1)
    txt1 = "soc-pokec-profiles.txt"

    with gzip.open(file_name1, 'rb') as f_in:
        with open(txt1, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)

    os.remove("soc-pokec-profiles.txt.gz")

    print("Downloading pokec dataset finished..")

if __name__ == "__main__":

    downloadDataset()
