# ASSS-Hent-Data
KS sin gamle løsning for å gjøre spørringer mot SSB sitt API var stort, rotete og krevde at det i noen tilfeller måtte gjøres justeringer i koden for å få den fungerende med en ny tabell.
Den brukte også STP (Stats to pandas) package i python som ikke hadde blitt oppdatert på en stund.

Grunnen til at vi bestemte oss for å gå vekk fra STP var to delt:
1. Vi var bekymret, for at en dag så kom pyjstat eller pandas til å deprecate en funksjon som stp brukte.
2. Med sammenslåing av kommuner og fylker så endte SSB med å legge inn nye kommuner og fylker inn i gamle tabeller.
Med det valget så vil spørringer som inkluderer regioner for et år når nye regioner og fylkeskoder ikke fantes så vil de bare dukke opp med value 0.
Det gjorde at vi satt igjen med utrolig mange rader med verdier vi ikke har bruk for.

Så derfor besemte vi oss for å lage en egen løsning som ikke bare gjør spørringer, men også filtrer ut regioner som ikke er gyldige for gitte år.
Vi testet ut litt forskjellige løsninger før vi kom frem til en vi skal gå for.

Først (Data filter alle aar) testet vi ut å hente alle radene for tabellen, så filtrere ut ugyldige regioner. Dette var den dårligste løsningen, da for noen tabeller så tok det dobbelt så lang tid.

Andre (Meta filter alle aar) løsningen var å gå gjennom år for år og filtrere ut regionskoder som ikke var gyldige for det året. For så å gjøre spørring per år istedenfor, vi endte opp med litt flere
spørringer. Denne løsningen brukte ca like lang tid som den gamle løsningen, men filtrerte ut regionskoder som ikke var gyldige for det året i tilegg.

Basert på den andre løsnigen vår (Meta Thread Filter Alle Aar), så brukte vi python sitt multiprocessing for å splitte opp pyjstat sin from_json_stat funksjon. Med den største tabellen vi henter ut, 
så gikk vi fra 300-400s til rundt 170s. from_json_stat funksjonen gikk fra rundt 150-160s ned til 15s. 
Denne fungerte dessverre ikke med MS SQL Server sin external_script funksjon og ga pickle error. Vi har et håp om at vi finner ut av dette en dag, men for nå så går vi videre med andre løsningen vår.

Veien videre etter testing av den andre løsningen og at vi fortsatt får riktig data fra spørringene våre, så har vi planer om å gjøre den til package andre kan importere og bruke.
