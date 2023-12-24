

from scrapinghelp import htmlhelper
import requests
import os
import json
from multiprocessing.pool import ThreadPool as Pool
from datetime import datetime
from pytz import timezone
import DBConfig


class data:

    def data(_Zip: str, _JobNumber: str, _Cookie: str):
        print("Data Started")
        filename = os.path.dirname(__file__) + "/Log/" + str(_JobNumber) + "/mainlink_" + str(_JobNumber) + ".txt"


        infile = open(filename, 'r')
        mainlink_list = json.load(infile)
        infile.close()

        print("Mainlink Loaded to a list")
        pool_size = 100
        pool = Pool(pool_size)
        for link in mainlink_list:
            # data.getdatafromweb(link, _Cookie, _JobNumber, mainlink_list)
            pool.apply_async(data.getdatafromweb,(link, _Cookie, _JobNumber, mainlink_list))
        pool.close()
        pool.join()
        print("Data End")

    def getdatafromweb(link, _Cookie, _JobNumber, mainlink_list):

        id = link["id"]
        if id == 55:
            v = 5
        mainlink = link["mainlink"]
        cat1 = link["cat1"]
        if cat1 == 'Beer>Beer Style':
            value = 'z'
        if mainlink != '':

            headers = {
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
                'Accept-Encoding': 'gzip, deflate, br',
                'Accept-Language': 'en-US,en;q=0.9',
                'Cache-Control': 'max-age=0',
                'Connection': 'keep-alive',
                # 'Cookie': _Cookie,
                # 'Host': 'www.calvertwoodley.com',
                # 'Referer': 'https://www.calvertwoodley.com/',
                'sec-ch-ua': '"Google Chrome";v="107", "Chromium";v="107", "Not=A?Brand";v="24"',
                'sec-ch-ua-mobile': '?0',
                'sec-ch-ua-platform': '"Windows"',
                'Sec-Fetch-Dest': 'document',
                'Sec-Fetch-Mode': 'navigate',
                'Sec-Fetch-Site': 'cross-site',
                'Sec-Fetch-User': '?1',
                # 'Upgrade-Insecure-Requests': '1',
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36'
            }

            totalcount = ""
            noofpage = 1
            ProducturlJson=""
            i = 1
            while i <= noofpage:
                outfilename = os.path.dirname(__file__) + "/Log/" + str(_JobNumber) + "/Data_" + str(id) + "_" + str(
                    i) + ".txt"

                # if '?' in mainlink:
                #         ProducturlJson = mainlink + "&page=" + str(i) + "&sortby=sort_item_order&l=100"
                #     # print(ProducturlJson)
                #
                # else:
                #     ProducturlJson = mainlink + "?page=" + str(i) + "&sortby=sort_item_order&l=100"
                #     # print(ProducturlJson)

                if cat1 == '90pt Wines>90+ Points Under $20':
                    ProducturlJson = mainlink

                elif cat1 == '90pt Wines>90-93 Points' or cat1 == '90pt Wines>94-96 Points' or cat1 == '90pt Wines>97-100 Points':
                    ProducturlJson = mainlink + "index.html?page=" + str(i) + "&sortby=winery&l=100"
                    # print(ProducturlJson)

                elif cat1 == 'Spirits on Sale':
                    if 'https://www.calvertwoodley.com/spirits-on-sale/' in mainlink:
                        ProducturlJson = mainlink.replace('https://www.calvertwoodley.comhttps://', '') + "?page=" + str(
                            i) + "&sortby=sort_item_order&l=100&item_type=spirits"

                # elif cat1 =='Beer>':
                #     mid = htmlhelper.returnvalue(mainlink, 'subregion=', '&')
                #     mainlink1 = "https://www.calvertwoodley.com/websearch_results.html?page=" + str(
                #         i) + "&sortby=winery&subregion=" + str(mid) + "&l=100&item_type=beer"
                #     ProducturlJson = mainlink1
                #
                elif cat1 == 'Beer>Style>':
                    cat_brk = cat1.split('>')
                    cat3 = cat_brk[2]
                    mainlink = "https://www.calvertwoodley.com/websearch_results.html?page=" + str(
                               i) + "&sortby=winery&subregion=" + str(cat3) + "&l=100&item_type=beer"



                else:
                    if'?' in mainlink:
                        ProducturlJson = mainlink + "&page=" + str(i) + "&sortby=sort_item_order&l=100"
                        print(ProducturlJson)

                    else:
                        ProducturlJson = mainlink + "?page=" + str(i) + "&sortby=sort_item_order&l=100"
                        print(ProducturlJson)

                ProducturlJsonsource = ""

                if DBConfig.proxy == 'Netnut':
                    proxies = {
                        'http': 'http://intrics-cc-intus-sid:MpyRwi4gJpZP@gw.ntnt.io:5959',
                        'https': 'http://intrics-cc-intus-sid:MpyRwi4gJpZP@gw.ntnt.io:5959'}
                if DBConfig.proxy == 'us-proxymesh':
                    proxies = {
                        'http': 'http://us.proxymesh.com:31280',
                        'https': 'http://us.proxymesh.com:31280'}

                while ProducturlJsonsource == "" or ProducturlJsonsource.status_code != 200:
                    try:
                        ProducturlJsonsource = requests.get(ProducturlJson, headers=headers, proxies=proxies,verify=False)
                        # print(ProducturlJsonsource)
                        if ProducturlJsonsource.status_code != 200:
                            print(str(ProducturlJsonsource) + " : " + ProducturlJson)
                    except:
                        ProducturlJsonsource = ""

                if i == 1:
                    totalcount = htmlhelper.returnvalueafter(ProducturlJsonsource.text, "Viewing", "of", '<').replace('&nbsp;|&nbsp;', '').strip()
                    # print(cat1, totalcount)
                    if totalcount != "":
                        noofpage = htmlhelper.returnnumberofpages(int(totalcount), 100)
                        # print(noofpage)

                print("Scrapping: id-" + str(id) + "/" + str(
                    len(mainlink_list)) + " totalcount-" + totalcount + " pages-" + str(i) + "/" + str(
                    noofpage) + " - Time-" + str(datetime.now()))

                fileout = open(outfilename, 'w')

                DATE = "TIMESTAMP:[" + datetime.now(timezone('US/Eastern')).strftime("%m/%d/%Y %I:%M:%S %p") + "]"

                json.dump(DATE + ProducturlJsonsource.text, fileout)

                fileout.close()

                i = i + 1
