

from scrapinghelp import htmlhelper
import requests
import os
import json
from multiprocessing.pool import ThreadPool as Pool
from datetime import date
from datetime import datetime
from pytz import timezone
from unit_uom import uom_uom_pack
finaldatalist = []
sql_data = []
count=0

class extracfinal:
        def extracfinal(_Zip,_JobNumber,_Cookie):
            global product_list
            global finaldatalist
            print("ExtractFinalData Started : " + str(datetime.now()))
            filename1 = os.path.dirname(__file__) + "/Csv/"
            if not os.path.exists(filename1):
                os.makedirs(filename1)
            filename1 = filename1 + str(_JobNumber) + ".csv"
            if os.path.exists(filename1):
                os.remove(filename1)


            finaldatalist.append(htmlhelper.addheader(filename1))
            filepath = ""


            filename = os.path.dirname(__file__) + "/Log/" + str(_JobNumber) + "/productlink_" + str(_JobNumber) + ".txt"
            infile = open(filename, 'r')
            mainlink_list = json.load(infile)
            infile.close()

            pool_size = 1
            pool = Pool(pool_size)
            for link in mainlink_list:
                start = 1
                # extracfinal.readdata(link, start, _Zip, _JobNumber, _Cookie, filename1)
                pool.apply_async(extracfinal.readdata(link, start, _Zip, _JobNumber,_Cookie, filename1))
            pool.close()
            pool.join()
            f = open(filename1, 'w')
            f.writelines(finaldatalist)
            f.close()
            htmlhelper.insert_data('Calvertwoodley_data', sql_data)


        def readdata(link,start,_Zip:str,_JobNumber:str,_Cookie:str,filename1: str):
            global _counter
            id = link["id"]
            category = link["cat1"]
            if category=='Wine>Type>Red':
                value='z'
            datafilename = os.path.dirname(__file__) + "/Log/" + str(_JobNumber) + "/Product_Data_" + str(id) + ".txt"

            if os.path.exists(datafilename):
                infile = open(datafilename, 'r')
                datafromproduct = str(infile.read()).replace('\\', '')
                datafromproduct = htmlhelper.returnformatedhtml(datafromproduct)
                today = date.today()
                _Date = today.strftime("%m/%d/%Y")
                # _Date = htmlhelper.returnvalue(datafromproduct,"TIMESTAMP:[","]" )
                extracfinal.extract(datafromproduct,start, category, _Zip, _Date, _JobNumber, filename1)
                infile.close()

        def extract(Product: str, id: int, category: str, _Zip: str, _Date: str, _JobNumber: str,
                    filename1: str ):
            # global _counter
            ID = ""
            SITE = "https://www.calvertwoodley.com"
            ZIP = _Zip
            DATE = _Date
            ALTPRICE = ""
            CATEGORY = category
            PRICE = ""
            SALE_ENDDATE = ""
            PRICE_MULT = ""
            EINDICATOR = ""
            ALTPRICEMULT = ""
            ALTINDICATOR = ""
            ASIN = ""
            RATING = ""
            VALID = ""
            AISLE = ""
            SIZE = ""
            DESCRIPTION=""
            PRODUCT_ID=""
            ORIG_UPC=""
            UPC=""
            PAGEIMAGELINK=""
            IMAGE_LINK=""
            EPRICE=""
            EALTPRICE=""
            NOTE=""



            DESCRIPTION = htmlhelper.returnvalue(Product, '"colr heading">', "<").replace('u00f1','n').replace('u00e9','e').replace('&#39;','\'').replace('u00f4','o').replace('u00ed','i').replace('u00e2','a').replace('u00e8','e').replace('u00eb','e').replace('u00f3','o').replace('u00e1','a').replace('00faa','a').replace('u00e0','a').replace('u00e3','a').replace('u00ea','e').replace('u00f6','o').replace('u00e7','c').replace('u00ef','i').replace('00f9r','r').replace('u00fc','u').replace('u00c9','E').replace('u00a0y','y').replace('u00ee','i').replace('u00f2','u')
            DESCRIPTION = DESCRIPTION.replace('u00fa','o').replace('u00fb','u').replace('u00f9','u').replace('u00ff','y').replace('u00ec','i').replace('u00e4','a').replace('u00b0','°').replace('u0096','–').replace('u00da','U').replace('00f9','').replace('u00f2','o').replace('u00ec','i').replace('00fa','').replace('u00fb','u')
            # print(DESCRIPTION)

            shrt1 = htmlhelper.returnvalue(Product, "itemprop=\"sku\"", "/td>")

            PRODUCT_ID = htmlhelper.returnvalue(shrt1, ">", "<")
            if PRODUCT_ID=='55650':
                value='z'

            ORIG_UPC = (htmlhelper.returnvalue(Product, 'itemprop="gtin8" content="', '"'))
            # print(ORIG_UPC)

            UPC = ORIG_UPC
            # print(UPC)

            sht = htmlhelper.returnvalue(Product, "class=\"pimg\"", "title=")
            PAGEIMAGELINK = htmlhelper.returnvalue(Product, "property=\"og:url\" content=\"", "\"")


            IMAGE_LINK = "https://www.calvertwoodley.com" + htmlhelper.returnvalue(Product, " class=\"pimg\" href=\"","\"")
            if IMAGE_LINK=='https://www.calvertwoodley.com':
                value='z'

            EPRICE = htmlhelper.returnvalue(Product,'itemprop="price" content="','">')

            if (EPRICE != ""):
                PRICE = '$' + EPRICE

            # EALTPRICE = htmlhelper.returnvalue(Product, "property=\"og:price:amount\" content=\"", "\">")
            # print(EALTPRICE)
            ALTPRICE_SRC = htmlhelper.returnvalueafter(Product, 'itemprop="price"', '<tr>', '/span>')
            EALTPRICE = htmlhelper.returnvalueafter(ALTPRICE_SRC, "Unit", "text-decoration: line-through;'>", "<")

            # EALTPRICE = htmlhelper.returnvalue(Product,"<td width=\"70\"><span style='color: #000; text-decoration: line-through;'>$","</span>")
            if (EALTPRICE != ""):
                ALTPRICE = '$' + EALTPRICE
                ALTPRICE = ALTPRICE.replace("$$", "$")
                EALTPRICE = EALTPRICE.replace('$','')

            NOTE = htmlhelper.returnvalueafter(Product, "class='prodata_cat'>Producer", '<span>', "</span>").replace('u00e9','e').replace('u00f3','o').replace('u00e1','a').replace('00faa','a').replace('u00e0','a').replace('u00e3','a').replace('u00ea','e').replace('u00f6','o').replace('u00e7','c').replace('u00ef','i').replace('00f9r','r').replace('u00fc','u').replace('u00c9','E')
            NOTE = NOTE.replace("u00b0", "°").replace("u00da", "U").replace("u0096", "-").replace("u00ec", "i").replace(
                "u00ef", "i").replace("u0092", "'").replace("u00e3", "a").replace("u00f1", "n").replace("u00ea",
                                                                                                        "e").replace(
                "u00ff", "y").replace("u00f2", "o").replace("u00fc", "u").replace("u00e4", "a").replace("u00e1",
                                                                                                        "a").replace(
                "u00e7", "c").replace("u00ee", "i").replace("u00ed", "i").replace("u00c9", "E").replace("u00f9", "u").replace("  ", " ")
            NOTE = NOTE.replace('u00f1','n').replace('u00e9','e').replace('&#39;','\'').replace('u00f4','o').replace('u00ed','i').replace('u00e2','a').replace('u00e8','e').replace('u00eb','e').replace('u00f3','o').replace('u00e1','a').replace('00faa','a').replace('u00e0','a').replace('u00e3','a').replace('u00ea','e').replace('u00f6','o').replace('u00e7','c').replace('u00ef','i').replace('00f9r','r').replace('u00fc','u').replace('u00c9','E').replace('u00a0y','y').replace('u00ee','i')

            PACK = ""
            UOM = ""
            UNIT = ""

            if DESCRIPTION!='':
                unituomvalues = uom_uom_pack.get_unit_uom(DESCRIPTION)
                PACK = str(unituomvalues["pack"]).strip()
                UOM = str(unituomvalues["uom"]).strip()
                UNIT = str(unituomvalues["unit"]).strip()
            if (not ORIG_UPC.isdigit()):
                ORIG_UPC = " "
                UPC = " "


            if (PRICE != "" and EPRICE != ""):
                PRICE_MULT = "1"
            if (ALTPRICE != "" and EALTPRICE != ""):
                ALTPRICEMULT = "1"
            if (PRICE != "" and ALTPRICE != ""):
                EINDICATOR = "*"
            if PRICE==ALTPRICE:
                ALTPRICE=''
                EALTPRICE=''
                EINDICATOR=''
            if ALTPRICE=='':
                EPRICE = htmlhelper.returnvalue(Product, "property=\"og:price:amount\" content=\"", "\">")
                if (EPRICE != ""):
                    PRICE = '$' + EPRICE


            if '(1.75L)' in DESCRIPTION and UNIT == '' and UOM == '':
                UNIT = '1.75'
                UOM = 'L'
            if '(1.5L)' in DESCRIPTION and UNIT == '' and UOM == '':
                UNIT = '1.5'
                UOM = 'L'
            if '(5L)' in DESCRIPTION and UNIT == '' and UOM == '':
                UNIT = '5'
                UOM = 'L'
            if '(4L)' in DESCRIPTION and UNIT == '' and UOM == '':
                UNIT = '4'
                UOM = 'L'
            if '(3L)' in DESCRIPTION and UNIT == '' and UOM == '':
                UNIT = '3'
                UOM = 'L'
            if '(1L)' in DESCRIPTION and UNIT == '' and UOM == '':
                UNIT = '1'
                UOM = 'L'
            if '(Each)' in DESCRIPTION and UNIT == '' and UOM == '':
                UNIT = ''
                UOM = 'Each'

            if '(4 pack cans)' in DESCRIPTION and UNIT == '' and UOM == '':
                UNIT=''
                UOM=''


            global count
            count = count + 1

            ID = count
            listdata = htmlhelper.createpsv( ID, SITE, ZIP, DATE, IMAGE_LINK, PRICE, PRICE_MULT, EPRICE,
                                            EINDICATOR, ALTPRICE, ALTPRICEMULT, EALTPRICE, ALTINDICATOR, PACK, UNIT,
                                            UOM,
                                            SIZE, PRODUCT_ID, UPC, ASIN, CATEGORY, DESCRIPTION, NOTE, RATING, VALID,
                                            PAGEIMAGELINK, AISLE, ORIG_UPC, _JobNumber, SALE_ENDDATE, '',
                                                     '', '',
                                                     '', '',
                                                     '', '',
                                                     '', '', '', '')

            print(listdata)


            data_tuple = htmlhelper.create_data_list(SITE, ZIP, DATE, IMAGE_LINK, PRICE, PRICE_MULT, EPRICE, EINDICATOR,
                                                     ALTPRICE, ALTPRICEMULT, EALTPRICE, ALTINDICATOR, PACK, UNIT, UOM,
                                                     SIZE, PRODUCT_ID, UPC, ASIN, CATEGORY, DESCRIPTION, NOTE, RATING,
                                                     VALID, PAGEIMAGELINK, AISLE, ORIG_UPC, _JobNumber, SALE_ENDDATE, '',
                                                     '', '',
                                                     '', '',
                                                     '', '',
                                                     '', '', '','')
            sql_data.append(data_tuple)
            finaldatalist.append(listdata)