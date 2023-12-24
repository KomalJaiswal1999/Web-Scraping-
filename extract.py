
import json
import os
from datetime import datetime
from multiprocessing.pool import ThreadPool as Pool
from scrapinghelp import htmlhelper

ProductlinkList = []
count = 0


class extractdata:

    def extractdata(_Zip: str, _JobNumber: str, _Cookie: str):
        print("ExtractData Started : " + str(datetime.now()))

        filename = os.path.dirname(__file__) + "/Log/" + str(_JobNumber) + "/mainlink_" + str(_JobNumber) + ".txt"
        infile = open(filename, 'r')
        mainlink_list = json.load(infile)
        infile.close()

        pool_size = 100
        pool = Pool(pool_size)
        for link in mainlink_list:
            if link=='https://www.calvertwoodley.com/topics/90-Points-Under-20-g447276354':
                value='z'
            start = 1
            # extractdata.readdatafromfile(link, start, _Zip, _JobNumber)
            pool.apply_async(extractdata.readdatafromfile, (link, start, _Zip, _JobNumber,))
        pool.close()
        pool.join()

        filename = "productlink_" + str(_JobNumber)
        directory = os.path.dirname(__file__) + "/Log/" + str(_JobNumber) + "/"  # code of making folder
        if not os.path.exists(directory):
            os.makedirs(directory)
        try:
            if os.path.exists(directory):
                if os.path.exists(directory + filename + ".txt"):
                    os.remove(directory + filename + ".txt")
        except OSError:
            print('Error: deleting. ' + directory)

        fileout = open(directory + filename + ".txt", 'w')
        json.dump(ProductlinkList, fileout)
        fileout.close()

        print("ExtractData End : " + str(datetime.now()) + " with Productlink count : " + str(count))

    def readdatafromfile(link, start: int, _Zip: str, _JobNumber: str):
        global count
        id = link["id"]
        category = link["cat1"]
        datafilename = os.path.dirname(__file__) + "/Log/" + str(_JobNumber) + "/Data_" + str(id) + "_" + str(start) + ".txt"

        if os.path.exists(datafilename):
            infile = open(datafilename, 'r')
            datafromfile = str(infile.read()).replace('\\', '')
            _Date = htmlhelper.returnvalue(datafromfile, "TIMESTAMP:[", "]")

            ProductArr = htmlhelper.collecturl(datafromfile, '', 'class="helper', 'title="')
            # print(datafilename,len(ProductArr))
            for i in range(len(ProductArr)):
                count = count + 1
                productlink = "https://www.calvertwoodley.com" + htmlhelper.returnvalue(ProductArr[i], 'href="', '"')
                ProductlinkList.append(
                    htmlhelper.mainlinksinsert(count, 'date', 'zip', productlink, category, "", "", "", "",
                                               "Valid", "", "", "", ""))

            infile.close()
            start = start + 1
            extractdata.readdatafromfile(link, start, _Zip, _JobNumber)
            # extractdata.readdatafromfile(link, start, _Zip, _JobNumber, _Cookie, filepath)
