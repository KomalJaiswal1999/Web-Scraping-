

import requests
from scrapinghelp import htmlhelper
import os
import json
import datetime

global mainlink_list


mainlink_list = []
count = 0




class mainlink:

    def mainlink(_Zip: str, _JobNumber: str, _Cookie: str):
        global count
        headers = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'en-US,en;q=0.9',
            'Cache-Control': 'max-age=0',
            'Connection': 'keep-alive',
            'Cookie': _Cookie,
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
        requestUrl = 'https://www.calvertwoodley.com/'
        response = requests.get(requestUrl, headers=headers)
        ResponseInString = htmlhelper.returnformatedhtml(response.text)
        short_code = htmlhelper.returnvalue(ResponseInString, 'ul id="topnav" class="sf-menu"', 'alt="Tastings"')   #332,608
        category_collecting = htmlhelper.collecturl(short_code, "", "<a alt=\"", "</ul></li><li ")
        for i in category_collecting:
            cat1 = htmlhelper.returnvalue(i, '" title="', '" href="/')
            if cat1 == "Spirits":
                v=1

            if cat1 == 'Wine' or cat1 == 'Beer':
                # cat_coll = htmlhelper.collecturl(i, "",'<li><a href="#"','</a></li></ul></li>')
                cat_coll = htmlhelper.collecturl(i, "", '<li><a href="#"', '</ul></li>')
                if cat1 == 'Beer':
                    cat_sor = htmlhelper.returnvalue(i+'&&', 'varietal">Show more</a></li></ul></li>', '&&')
                    c_coll = htmlhelper.collecturl(cat_sor, '', 'href="', '/a>')
                    if c_coll:
                        cat_coll = cat_coll + c_coll



                for j in cat_coll:
                    cat2 = htmlhelper.returnvalue(j, '>', '<')
                    cat2Url= "https://www.calvertwoodley.com"+htmlhelper.returnvalue(j,'href="','"')
                    # print(cat2)

                    if cat2 == 'Type' and cat1 == 'Wine':
                        cat_coll2 = htmlhelper.collecturl(j, "", '<li>', '</li>')
                        # print(cat_coll2)

                        for k in cat_coll2:
                            cat3 = htmlhelper.returnvalue(k, '">', '<')
                            cat3Url = "https://www.calvertwoodley.com" + htmlhelper.returnvalue(k, 'href="', '"')
                            print(cat3)

                            count = count + 1

                            finalcategory = ""
                            finalcategory = cat1 + ">" + cat2 + ">" + cat3

                            print(finalcategory, count, cat3Url)
                            mainlink_list.append(
                                htmlhelper.mainlinksinsert(count, 'date', 'zip', cat3Url, finalcategory, "", "", "", "",
                                                           "Valid", "", "", "", ""))



                    elif cat2 == 'Beer Style' and cat1 == 'Beer':

                        url = "https://www.calvertwoodley.com/beer/?m=subregion"
                        responseForBeer = requests.get(url, headers=headers)
                        responseForBeerhtml = htmlhelper.returnformatedhtml(responseForBeer.text)
                        beershortsource = htmlhelper.returnvalue(responseForBeerhtml, 'id="subregion_long">', '</ul>')

                        cat_coll2 = htmlhelper.collecturl(beershortsource, "", "<li>", "</li>")

                        for l in cat_coll2:
                            cat3 = htmlhelper.returnvalue(l, 'class="subexpandable">', '<')
                            cat3Url = "https://www.calvertwoodley.com/websearch_results.html?subregion=" + htmlhelper.returnvalue(l, 'subregion=', '&')
                            count = count + 1
                            finalcategory = ""
                            finalcategory = cat1 + ">" + cat2 + ">" + cat3
                            print(finalcategory, count, cat3Url)
                            mainlink_list.append(
                                htmlhelper.mainlinksinsert(count, 'date', 'zip', cat3Url, finalcategory, "", "", "", "","Valid", "", "", "", ""))





            else:
                i = i + '/a></li>'
                cat_coll = htmlhelper.collecturl(i, '', '<li><a ', '/a></li>')
                # cat_coll = htmlhelper.collecturl(i, '', '<li><a ', '/a>')

                # print(cat_coll)
                for m in cat_coll:
                    cat2 = htmlhelper.returnvalue(m, ">", "<")
                    cat2Url = "https://www.calvertwoodley.com" + htmlhelper.returnvalue(m, 'href="', '"')
                    if'https://www.calvertwoodley.comhttps://www.calvertwoodley.com/' in cat2Url:
                        cat2Url=cat2Url.replace('https://www.calvertwoodley.comhttps://www.calvertwoodley.com','https://www.calvertwoodley.com')
                        print(cat2Url)
                    # print(cat_coll)

                    count = count + 1
                    print(count)
                    finalcategory = ""
                    finalcategory = cat1 + ">" + cat2
                    print(finalcategory, count, cat2Url)
                    mainlink_list.append(
                        htmlhelper.mainlinksinsert(count, 'date', 'zip', cat2Url, finalcategory, "", "", "", "",
                                                   "Valid", "", "", "", ""))

        filename = "/mainlink_" + str(_JobNumber)
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
        json.dump(mainlink_list, fileout)
        fileout.close()
