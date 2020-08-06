import pandas as pd
from pyjstat import pyjstat
import requests
from collections import OrderedDict
import stats_to_pandas as stp
import time
import copy
import re


class SSBTable:
    def __init__(self, tabell_id):
        self.tabell_id = tabell_id
        # self.queries = []

    @property
    def url(self):
        full_url = "http://data.ssb.no/api/v0/no/table/"+self.tabell_id
        return full_url

    @property
    def variables(self):
        full_url = self.url
        df = pd.read_json(full_url)
        SSBvariables = [dict(values) for values in df.iloc[:, 1]]
        return SSBvariables

    @property
    def dimensions(self):
        l = [i["code"] for i in self.variables]
        return l

    @property
    def metadata(self):
        dfs = []
        for i in range(len(self.dimensions)):
            dfs.append(pd.DataFrame({str(self.dimensions[i])+"_kode": self.variables[i]["values"], str(self.dimensions[i]): self.variables[i]["valueTexts"]}))
        return dfs


def read_query(queries):
    dataframes = []
    for i in queries:
        data = requests.post(a.url, json=i)
        results = pyjstat.from_json_stat(data.json(object_pairs_hook=OrderedDict), naming="id")
        dataframes.append(results[0])
    if len(queries) > 1:
        big_df = pd.concat(dataframes, ignore_index=True)
        return big_df
    else:
        return dataframes[0]


x = '07459'
a = SSBTable(x)

query1 = stp.full_json(table_id=x, language="no")

datostring = time.strftime("%Y, %m, %d, %H, %M, %S")
datosplit = datostring.split(", ")

# År er alltid siste dimensjonen i tabellen og siste verdi i arrayet har siste gyldige året
if x != "08655":
    gyldigeAar = query1["query"][-1]["selection"]["values"]
    forsteAar = int(gyldigeAar[0])

# forsteAar = int(query1["query"][-1]["selection"]["values"][0])
# År er alltid siste dimensjonen i tabellen og siste verdi i arrayet har siste gyldige året
if x != "08655":
    sisteAar = int(query1["query"][-1]["selection"]["values"][-1])

# Year variables used to get the years from 2015-now
aarsFilter = "top"
antallSisteAar = ["4"]

# Each year consists of 4 quarters, which means that we want the top 12 to get the last 3 years
antallKvartalAar = ["12"]

if x == "12367":
    query = []
    for i in a.variables[a.dimensions.index("KOKart0000")]["values"]:
        d = {"query": [{"code": "KOKregnskapsomfa0000", "selection": {"filter": "item", "values": ["A"]}},
                       {"code": "KOKart0000", "selection": {"filter": "item", "values": [i]}},
                       {"code": "Tid", "selection": {"filter": "top", "values": ["3"]}}], "response": {"format": "json-stat2"}}
        query.append(d)
elif x == "09817":
    query = []
    kommuneListe = query1["query"][0]

    # The first landbakgrunn variable is Alle Land, pop this to use it later
    alleLandVar = query1["query"][2]["selection"]["values"].pop(0)
    landbakgrunnListe = query1["query"][2]

    for i in a.variables[a.dimensions.index("InnvandrKat")]["values"]:
        d = {"query": [kommuneListe,
                       {"code": "Landbakgrunn", "selection": {
                           "filter": "item", "values": [alleLandVar]}},
                       {"code": "InnvandrKat", "selection": {
                           "filter": "item", "values": [i]}},
                       {"code": "ContentsCode", "selection": {
                           "filter": "item", "values": ["Personer1"]}},
                       {"code": "Tid", "selection": {"filter": aarsFilter, "values": antallSisteAar}}],
             "response": {"format": "json-stat2"}}

        query.append(d)

        # If innvandringskategori is Invandrere then we want all the single countries as well
        if i == "B":
            d = {"query": [kommuneListe,
                           landbakgrunnListe,
                           {"code": "InnvandrKat", "selection": {
                               "filter": "item", "values": [i]}},
                           {"code": "ContentsCode", "selection": {
                               "filter": "item", "values": ["Personer1"]}},
                           {"code": "Tid", "selection": {"filter": aarsFilter, "values": antallSisteAar}}],
                 "response": {"format": "json-stat2"}}
            query.append(d)
elif x == "07984":
    query = []
    kommuneListe = query1["query"][0]
    næringListe = query1["query"][1]
    alderListe = query1["query"][3]

    q = {"query": [kommuneListe,
                   næringListe,
                   alderListe,
                   {"code": "Tid", "selection": {"filter": aarsFilter, "values": antallSisteAar}}],
         "response": {"format": "json-stat2"}}
    query.append(q)
elif x == "12449":
    query1["query"][-1]["selection"]["filter"] = aarsFilter
    query1["query"][-1]["selection"]["values"] = antallKvartalAar
    query = [query1]
elif x == "12362":
    # The standard form to get KOSTRA funksjons from SSB
    funksjonListe = {"code": "KOKfunksjon0000",
                     "selection": {"filter": "item", "values": []}}
    kommuneListe = query1["query"][0]

    # Want all the FGK funksjons so we add the ones that starts with FGK to funksjonListe
    for i in a.variables[a.dimensions.index("KOKfunksjon0000")]["values"]:
        if i.startswith("FGK"):
            funksjonListe["selection"]["values"].append(i)

    q = {"query": [kommuneListe,
                   funksjonListe,
                   {"code": "KOKart0000", "selection": {
                       "filter": "item", "values": ["AGD2"]}},
                   {"code": "Tid", "selection": {"filter": aarsFilter, "values": antallSisteAar}}],
         "response": {"format": "json-stat2"}}
    query = [q]
elif x == "12368":
    # The standard form to get KOSTRA funksjons from SSB
    funksjonListe = {"code": "KOKfunksjon0000",
                     "selection": {"filter": "item", "values": []}}
    kommuneListe = query1["query"][0]

    # Want all the FGK funksjons so we add the ones that starts with FGK to funksjonListe
    for i in a.variables[a.dimensions.index("KOKfunksjon0000")]["values"]:
        if i.startswith("FGF"):
            funksjonListe["selection"]["values"].append(i)

    q = {"query": [kommuneListe,
                   funksjonListe,
                   {"code": "KOKart0000", "selection": {
                       "filter": "item", "values": ["AGD2"]}},
                   {"code": "Tid", "selection": {"filter": aarsFilter, "values": antallSisteAar}}],
         "response": {"format": "json-stat2"}}
    query = [q]
elif x == "01182":
    query1["query"][-1]["selection"]["filter"] = aarsFilter
    query1["query"][-1]["selection"]["values"] = ["1"]
    query = [query1]
elif x == "08655":
    print("08655")
    query = [query1]
else:
    # Special cases, remove unwanted filter (bygningstype)
    if x == "05939" or x == "05940":
        del query1["query"][1]

    # if x == "09345":
    #	print(query1["query"][0]["selection"]["filter"])
    #	query1["query"][0]["selection"]["filter"] = "vs:Kommun"

    # Gamle koden
    # 2020 sammenslåingskodene
    # Svalbard, Jan Mayen, Kontinentalsokkelen, Utlandet, Havområder, Ikke bosatt i Norge
    # Viken, Innlandet, Vestfold og Telemark, Agder, Vestland, Troms og Finnmark
    # komnrreg = re.compile(r"^21|^22|^23|^25|^26|^30|^34|^38|^42|^46|^54|^88")
    komnrreg = re.compile(r"^30|^34|^38|^42|^46|^54")
    """    
     Itererer på år for alle tabeller
     query = []
     for i in range(0, int(antallSisteAar[-1])):
    	gjeldendeAar = sisteAar - i
    
    	Hvis tabellen ikke har antallSisteAar antall år
    	if (gjeldendeAar < forsteAar):
    		break
    
    	query.append(copy.deepcopy(query1))
    	kom = []
    	if (gjeldendeAar < 2020):
    		for komnr in query[-1]["query"][0]["selection"]["values"]:
    			if not komnrreg.match(komnr):
    				kom.append(komnr)
    """
    regionindeks = 0
    for idx, content in enumerate(query1["query"]):
        if (content["code"] == "KOKkommuneregion0000" or content["code"] == "vs:Kommun"):
            regionindeks = idx
    """
    Bruker regex for å fjerne overflødig data fra query
    Viken, Innlandet, Vestfold og Telemark, Agder, Vestland, Troms og Finnmark
    """
    nykomreg = re.compile(r"^30|^34|^38|^42|^46|^54")

    trondregpre = re.compile(r"^16")
    trondregpost = re.compile(r"^50")
    """
    Disse ble ikke påvirket av 2020 sammenslåingen
    Trøndelag ble slått sammen i 2018 så man kunne hatt dette som noe eget
    Stavanger, Møre og Romsdal, Nordland, Trøndelag
    """
    uendretreg = re.compile(r"^11|^15|^18|^50")

    # Svalbard, Jan Mayen, Kontinentalsokkelen, Utlandet, Havområder, Ikke bosatt i Norge
    ovrigreg = re.compile(r"^21|^22|^23|^25|^26|^88|^99")

    # Itererer på år for alle tabeller
    # Ta utgangspunkt i gyldige år og går ned fra høyest til lavest, lager ett query per år
    query = []
    for i in enumerate(reversed(gyldigeAar)):
        gjeldendeAar = int(i[1])
        indeks = int(i[0])

        if (indeks + 1 > int(antallSisteAar[0])):
            break

        # Hvis tabellen ikke har antallSisteAar antall år
        if (gjeldendeAar < forsteAar):
            break

        # Deepcopier dataene hentet fra SSB for å kunne
        query.append(copy.deepcopy(a.variables))

        kom = []
        # Hvis året man henter ut er < 2020 ta bare med kommunedata på pre-2020 kommuner
        if (gjeldendeAar < 2018):
            for komnr in query[-1]["query"][regionindeks]["selection"]["values"]:
                if not ovrigreg.match(komnr):
                    if not nykomreg.match(komnr):
                        if not trondregpost.match(komnr):
                            if(komnr == "5001"):
                                print("wtf skjer her?2")
                            kom.append(komnr)
        elif (gjeldendeAar < 2020):
            for komnr in query[-1]["query"][regionindeks]["selection"]["values"]:
                if not ovrigreg.match(komnr):
                    if not nykomreg.match(komnr):
                        if not trondregpre.match(komnr):
                            if(komnr == "1601"):
                                print("wtf skjer her?2")
                            kom.append(komnr)
        # Ta bare med kommunedata på post-2020 kommuner
        else:
            for komnr in query[-1]["query"][regionindeks]["selection"]["values"]:
                if not ovrigreg.match(komnr):
                    if nykomreg.match(komnr) or uendretreg.match(komnr):
                        kom.append(komnr)

        query[-1]["query"][regionindeks]["selection"]["values"] = kom
        query[-1]["query"][-1]["selection"]["values"] = [str(gjeldendeAar)]
        query[-1]["response"]["format"] = "json-stat2"

tries = 0
r = read_query(query)
while (tries < 10 and r.empty):
    r = read_query(query)
    tries = tries + 1
