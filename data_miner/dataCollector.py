__author__ = 'blaz'

import MySQLdb
import json

import sys
reload(sys)
sys.setdefaultencoding("utf-8")

####################
#Spisek koordinat drzav, kamor bomo zapikali markerje
####################
coordinates = {"Argentina": {"latLng": [-34, -64], "name": 'Argentina'},\
               "Austria": {"latLng": [47.3333, 13.3333], "name": 'Austria'},\
               "Belgium": {"latLng": [50.8333, 4], "name": 'Belgium'},\
               "Canada": {"latLng": [60, -95], "name": 'Canada'},\
               "China": {"latLng": [35, 105], "name": 'China'},\
               "Croatia": {"latLng": [45.1667, 15.5], "name": 'Croatia'},\
               "Czech Republic": {"latLng": [49.75, 15.5], "name": 'Czech Republic'},\
               "Denmark": {"latLng": [56, 10], "name": 'Denmark'},\
               "England": {"latLng": [54, -2], "name": 'England'},\
               "Finland": {"latLng": [64, 26], "name": 'Finland'},\
               "France": {"latLng": [46, 2], "name": 'France'},\
               "Germany": {"latLng": [51, 9], "name": 'Germany'},\
               "Greece": {"latLng": [39, 22], "name": 'Greece'},\
               "Hungary": {"latLng": [47, 20], "name": 'Hungary'},\
               "Ireland": {"latLng": [53, -8], "name": 'Ireland'},\
               "Italy": {"latLng": [42.8333, 12.8333], "name": 'Italy'},\
               "Lithuania": {"latLng": [56, 24], "name": 'Lithuania'},\
               "Mexico": {"latLng": [23, -102], "name": 'Mexico'},\
               "Netherlands": {"latLng": [52.5, 5.75], "name": 'Netherlands'},\
               "Norway": {"latLng": [62, 10], "name": 'Norway'},\
               "Poland": {"latLng": [52, 20], "name": 'Poland'},\
               "Portugal": {"latLng": [39.5, -8], "name": 'Portugal'},\
               "Romania": {"latLng": [46, 25], "name": 'Romania'},\
               "Russia": {"latLng": [60, 100], "name": 'Russia'},\
               "Scotland": {"latLng": [54, -2], "name": 'Scotland'},\
               "Serbia": {"latLng": [44, 21], "name": 'Serbia'},\
               "Slovakia": {"latLng": [48.667, 19.5], "name": 'Slovakia'},\
               "Slovenia": {"latLng": [46.04, 14.33], "name": 'Slovenia'},\
               "Spain": {"latLng": [40, -4], "name": 'Spain'},\
               "Sweden": {"latLng": [62, 15], "name": 'Sweden'},\
               "Switzerland": {"latLng": [47, 8], "name": 'Switzerland'},\
               "Turkey": {"latLng": [39, 35], "name": 'Turkey'},\
               "Ukraine": {"latLng": [49, 32], "name": 'Ukraine'},\
               "United States": {"latLng": [38, -97], "name": 'United States'},\
               "Bulgaria": {"latLng": [43, 25], "name": 'Bulgaria'},\
               "Japan": {"latLng": [36, 138], "name": 'Japan'},\
               "Brasil": {"latLng": [-10, -55], "name": 'Brasil'},\
               "Uzbekistan": {"latLng": [41, 64], "name": 'Uzbekistan'},\
               "Egypt": {"latLng": [27, 30], "name": 'Egypt'}
}


####################
#Pridobi zeljene podatke iz podatkovne baze
####################
def getData(leto=1930, drzava="default", select = "ime, leto_izdaje, drzava_izdaje, leto_recepcije, drzava_recepcije"):

    #Nastavi drzavo
    if("default" in drzava):
        where = ""
    else:
        where = "and drzava_izdaje='"+drzava+"'"

    #povezi se na SQL bazo
    db = MySQLdb.connect(host="delphi.fri.uni-lj.si", # your host, usually localhost
                         user="studentgo", # your username
                          passwd="yP6YZcbSacFcCpuq", # your password
                          db="studentgo") # name of the data base
    cursor = db.cursor()
    cursor.execute("SELECT VERSION()")

    #data = cursor.fetchone()
    #print "Connected to database version : %s " % data

    #select podatkov
    sql = "SELECT " + select + " FROM avtorice where leto_izdaje<'"+str(leto)+"'"+ where + "and leto_izdaje<>''"
    try:
            cursor.execute(sql)
            results = cursor.fetchall()
            # disconnect from server
            db.close()
            return results
    except:
            print "getData: Operacije ni bilo mozno opraviti. Poredna panda!"

####################
#Obdelaj pridobljene podatke iz SQL in jih obdelaj
####################
def parseData(leto, drzava):

    #example usage
    #getData(1925,drzava="Denmark")

    results=getData(leto=leto,drzava=drzava)
    work_count = 0
    avtorice = []
    avtorice_izvor = {}
    avtorice_v_drzavi_recepcije = {}
    recenzije = {}
    markers = []
    stevec_recenzij = []
    obdelano_leto = []

    try:
        for row in results:
            ime = row[0]
            leto_izdaje = row[1]
            drzava_izdaje = row[2]
            leto_recepcije = row[3]
            drzava_recepcije = row[4]
            if "unknown" not in (drzava_izdaje or drzava_recepcije):
                #print "Ime avtorice: %s,Leto izdaje: %s, Drzava izdaje: %s, Leto Recepcije: %s, Drzava recepcije: %s" % \
                (ime, leto_izdaje, drzava_izdaje, leto_recepcije, drzava_recepcije)
                work_count+=1;

                #Stej katera avtorica je najbolj recenzirana v  svoji drzavi: Top 5
                try:
                    avtorice_izvor[ime]=avtorice_izvor[ime]+1
                except:
                    avtorice_izvor[ime]=1

                #Shrani avtorice v tabelo
                #prestej recenzije za tisti casovni razpon v posamezni drzavi
                if ime not in avtorice:
                    avtorice.append(ime)
                try:
                    recenzije[drzava_recepcije]=recenzije[drzava_recepcije]+1
                except KeyError:
                    recenzije[drzava_recepcije]=1

        #print "Stevilo recepcij: ", work_count
        #print "Avtorice: ", avtorice
        #print "Recenzije po drzavah: ", recenzije.keys()
        #print "Recenzije na Svedskem: ", recenzije['Sweden']
        #print "Stevilo recenzij: ", recenzije

        #Sortiranje dicta
        temp_author = []
        temp_value = []
        label_info = {}
        label_value = ""

        for key, value in sorted(avtorice_izvor.iteritems(), key=lambda (k,v): (v,k)):
            #print "%s: %s" % (key, value)

            #Odstrani karakterje, ki unicijo nas json
            #print key.translate(None, "',!\.;")
            key = key.translate(None, "',!\.;")
            temp_author.append(key)
            temp_value.append(value)

        if(len(temp_value)>=5):
            i=len(temp_value)-5
            #print temp_author
            #print temp_value
            for author in temp_author[-5:]:
                label_info[author]=temp_value[i]
                #label_value = label_value+author+" number of works: "+temp_value[i]+"</br>"
                label_value = label_value+author+", number of receptions: "+ str(temp_value[i])+ " </br>"
                i+=1
        else:
            i=0
            for author in temp_author:
                label_value = label_value+author+", number of receptions: "+ str(temp_value[i])+ " </br>"
                i+=1


        #Izpisi recenzije in markerje v obliki za v mapo
        for key,value in recenzije.items():
            #print key
            try:
                markers.append(coordinates[key])
                stevec_recenzij.append(value)
            except:
                #print ""
                continue
                #print "Country not found: ", key
        ##########################
        #Testni izpisi
        #print "Oblika za na label: ", label_value
        #print "Markers: ", markers
        #print "St recenzij: ", stevec_recenzij

        #Delujoce
        #obdelano_leto = {leto:{"coords": markers, "recepcions": stevec_recenzij}}

        #testno
        obdelano_leto = {leto:{"coords": markers, "recepcions": stevec_recenzij, "najboljse": label_value}}

        #print drzava," :", obdelano_leto

        return obdelano_leto

    except:
        print "parseData: Ni rezultatov, ali pa se je nekaj zalomilo! Bad panda!"

####################
#Poisci vse obstojece drzave, ki imajo vsaj eno delo
####################
def parseCounties(result=getData(select="drzava_izdaje")):
    countries = []
    exclude=['Austro-Hungarian Empire','Low Countries (French speaking)','Byzantine empire','Colonies of European countries','unknown / not relevant','Bohemia','Ottoman Empire','South-America (to be specified)','Belgium/Southern Netherlands']
    #print result
    try:
        for country in result:
            if (country[0] not in countries) and (country[0] not in exclude):
                countries.append(country[0])

        return countries
    except:
        print "parseCountries: Iskanje drzav ni uspelo. Poredna panda!"

####################
#Pretvori podatke v prikaz za mapo
#Za vsako drzavo z izdanimi deli preveri, kje ima recenzije in koliko
#####################
def createMapData(countries):

    data_json = {}
    stevec_drzav = 0

    # v tabelo drzav dodajamo v posamezen index stevilo recenzij za posamezna leta
    for country in countries:
        print "Processing: ", country
        tabela_drzav = [] #tabela vseh let za posamezno drzavo
        print_to_json = ""
        for year in range(1200,1926,25):
            tabela_drzav.append(parseData(str(year), country))

        year = 1200
        #dodamo vsa leta v koncne podatke
        for drzava in tabela_drzav:
            #print drzava[str(year)]
            if(year == 1200):
                print_to_json="{1200: "+str(drzava[str(year)])
            else:
                print_to_json = print_to_json+","+str(year)+": "+str(drzava[str(year)])
            year+=25
        print_to_json=print_to_json+"}"
        #print print_to_json
        print "Saving data for: ",country, " : ", print_to_json

        #USA FIX (different name on map and in data)
        if("United States" in country):
            country = "United States of America"

        data_json[country] = print_to_json
        #print "DATA: ", data_json

    print data_json

#TEST RUN
#
#print countries
#print len(countries)

#TEST OUTPUT
#parseData("Denmark", 1925)
#parseData(1300, "Netherlands")
#parseData(1930,"Russia")
#parseData(1930,"United States")

#parseData(1930,"default")

################
#Create map data
################
countries = parseCounties(result=getData(select="drzava_izdaje"))
#print countries
#print len(countries)
#countries = ["Netherlands"]
createMapData(countries)

#Test dodajanje markerjev
#print arrayOfMarkers['Argentina']
#testArray = []
#testArray.append(arrayOfMarkers['Argentina'])
#print testArray