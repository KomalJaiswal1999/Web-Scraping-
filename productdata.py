
from scrapinghelp import htmlhelper
import requests
import os
import json
from multiprocessing.pool import ThreadPool as Pool
from datetime import date
import DBConfig
import datetime


count = 1


class productdata:
    def productdata(_Zip: str, _JobNumber: str, _Cookie: str):
        print("Data Started")

        filename = os.path.dirname(__file__) + "/Log/" + str(_JobNumber) + "/productlink_" + str(_JobNumber) + ".txt"
        infile = open(filename, 'r')
        produc_list = json.load(infile)
        infile.close()
        print("Productlink Loaded to a list")
        pool_size = 75
        pool = Pool(pool_size)
        for link in produc_list:
            # productdata.getdatafromweb(link, _Zip,_Cookie, _JobNumber, produc_list)
            pool.apply_async(productdata.getdatafromweb, (link, _Zip,_Cookie, _JobNumber, produc_list,))
        pool.close()
        pool.join()
        print("Data End")

    def getdatafromweb(link, _Zip,_Cookie, _JobNumber, produc_list):
        id = link["id"]
        productlink = link["mainlink"]
        category = link["cat1"]
        outfilename = os.path.dirname(__file__) + "/Log/" + str(_JobNumber) + "/Product_Data_" + str(id) + ".txt"
        if not os.path.exists(outfilename):
            totalcount = ""
            noofpage = 1
            startpage = 1
            i = 1
            header = {
                # 'Cookie': _Cookie,
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
                'Accept-Encoding': 'gzip, deflate, br',
                'Accept-Language': 'en-US,en;q=0.9',
                'Cache-Control': 'max-age=0',
                'Connection': 'keep-alive',
                #'Cookie': _Cookie,
                'Host': 'www.calvertwoodley.com',
                'Referer': 'https://www.calvertwoodley.com/',
                'sec-ch-ua': '"Google Chrome";v="107", "Chromium";v="107", "Not=A?Brand";v="24"',
                'sec-ch-ua-mobile': '?0',
                'sec-ch-ua-platform': '"Windows"',
                'Sec-Fetch-Dest': 'document',
                'Sec-Fetch-Mode': 'navigate',
                'Sec-Fetch-Site': 'cross-site',
                'Sec-Fetch-User': '?1',
                'Upgrade-Insecure-Requests': '1',
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36'
            }

            ProducturlJson = productlink
            ProducturlJsonsource = ""
            while ProducturlJsonsource == "" or ProducturlJsonsource.status_code != 200:
                try:
                    ProducturlJsonsource = requests.get(ProducturlJson, headers=header, verify=False,proxies=proxies,timeout=100)
                    if ProducturlJsonsource.status_code != 200:
                        print(str(ProducturlJsonsource) + " : " + ProducturlJson)
                except:
                    ProducturlJsonsource = ""
            today = date.today()
            print("Scrapping: id-" + str(id) + "/" + str(len(produc_list)) + " - Time-" + str(ProducturlJsonsource.elapsed.total_seconds()))
            fileout = open(outfilename, 'w')
            DATE = "TIMESTAMP:" + today.strftime("%m/%d/%Y")
            json.dump(DATE + ProducturlJsonsource.text, fileout)
            fileout.close()
            startpage = startpage + 1
            i = i + 1

