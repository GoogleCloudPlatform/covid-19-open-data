[Back to main page](../README.md)

# Epidemiology
Information related to the COVID-19 infections for each date-region pair.


## URL
This table can be found at the following URLs depending on the choice of format:
* [epidemiology.csv](https://storage.googleapis.com/covid19-open-data/v3/epidemiology.csv)
* [epidemiology.json](https://storage.googleapis.com/covid19-open-data/v3/epidemiology.json)


## Schema
| Name | Type | Description | Example |
| ---- | ---- | ----------- | ------- |
| **date** | `string` | ISO 8601 date (YYYY-MM-DD) of the datapoint | 2020-03-30 |
| **key** | `string` | Unique string identifying the region | CN_HB |
| **new_confirmed<sup>1</sup>** | `integer` | Count of new cases confirmed after positive test on this date | 34 |
| **new_deceased<sup>1</sup>** | `integer` | Count of new deaths from a positive COVID-19 case on this date | 2 |
| **new_recovered<sup>1</sup>** | `integer` | Count of new recoveries from a positive COVID-19 case on this date | 13 |
| **new_tested<sup>2</sup>** | `integer` | Count of new COVID-19 tests performed on this date | 13 |
| **cumulative_confirmed<sup>3</sup>** | `integer` | Cumulative sum of cases confirmed after positive test to date | 6447 |
| **cumulative_deceased<sup>3</sup>** | `integer` | Cumulative sum of deaths from a positive COVID-19 case to date | 133 |
| **cumulative_recovered<sup>3</sup>** | `integer` | Cumulative sum of recoveries from a positive COVID-19 case to date | 133 |
| **cumulative_tested<sup>2,3</sup>** | `integer` | Cumulative sum of COVID-19 tests performed to date | 133 |

<sup>1</sup>Values can be negative, typically indicating a correction or an adjustment in the way
they were measured. For example, a case might have been incorrectly flagged as recovered one date so
it will be subtracted from the following date.\
<sup>2</sup>Some health authorities only report PCR testing. This variable usually refers to cumulative
number of tests and not tested persons, but some health authorities only report tested persons.\
<sup>3</sup>Cumulative count will not always amount to the sum of daily counts, because many authorities
make changes to criteria for counting cases, but not always make adjustments to the data. There is
also potential missing data. All of that makes the cumulative counts *drift* away from the sum of all
daily counts over time, which is why the cumulative values, if reported, are kept in a separate
column.


## Sources of data
<details>
<summary>Show data sources</summary>


| Data | Source | License and Terms of Use |
| ---- | ------ | ------------------------ |
| Country-level data | [ECDC](https://www.ecdc.europa.eu) | [Attribution required](https://www.ecdc.europa.eu/en/copyright) |
| Country-level data | [Our World in Data](https://ourworldindata.org) | [CC BY](https://ourworldindata.org/how-to-use-our-world-in-data#how-is-our-work-copyrighted) |
| Country-level data | [WHO](https://covid19.who.int) | [Attribution required](https://www.who.int/about/who-we-are/publishing-policies/data-policy/terms-and-conditions) |
| Afghanistan | [HDX](https://data.humdata.org/dataset/afghanistan-covid-19-statistics-per-province) | [CC BY][28] |
| Argentina | [Datos Argentina](https://datos.gob.ar/dataset/salud-covid-19-casos-registrados-republica-argentina) | [Public domain](https://datos.gob.ar/acerca/seccion/marco-legal) |
| Australia | [COVID LIVE](https://covidlive.com.au/) | [CC BY](https://creativecommons.org/licenses/by/4.0/) |
| Austria | [Open Data Österreich](https://www.data.gv.at/covid-19/) | [CC BY](https://www.data.gv.at/covid-19/) |
| Bangladesh | <http://covid19tracker.gov.bd> | [Public Domain](http://covid19tracker.gov.bd/#tab_1_4) |
| Belgium | [Belgian institute for health](https://epistat.wiv-isp.be) | [Attribution required](https://www.health.belgium.be/en/legal-information) |
| Brazil | [Brazil Ministério da Saúde](https://coronavirus.saude.gov.br/) | [Creative Commons Atribuição](http://www.opendefinition.org/licenses/cc-by) |
| Brazil (Rio de Janeiro) | <http://www.data.rio/> | [Dados abertos](https://www.data.rio/datasets/f314453b3a55434ea8c8e8caaa2d8db5) |
| Brazil (Ceará) | <https://saude.ce.gov.br> | [Dados abertos](https://cearatransparente.ce.gov.br/portal-da-transparencia) |
| Canada | [Department of Health Canada](https://www.canada.ca/en/public-health) | [Attribution required](https://www.canada.ca/en/transparency/terms.html) |
| Canada | [COVID-19 Canada Open Data Working Group](https://art-bd.shinyapps.io/covid19canada/) | [CC BY](https://github.com/ishaberry/Covid19Canada/blob/master/LICENSE.MD) |
| Chile | [Ministerio de Ciencia de Chile](http://www.minciencia.gob.cl/COVID19) | [Terms of use](http://www.minciencia.gob.cl/sites/default/files/1771596.pdf) |
| China | [DXY COVID-19 dataset](https://github.com/BlankerL/DXY-COVID-19-Data) | [MIT](https://github.com/BlankerL/DXY-COVID-19-Data/blob/master/LICENSE) |
| Colombia | [Datos Abiertos Colombia](https://www.datos.gov.co) | [Attribution required](https://herramientas.datos.gov.co/es/terms-and-conditions-es) |
| Czech Republic | [Ministry of Health of the Czech Republic](https://onemocneni-aktualne.mzcr.cz/covid-19) | [Open Data](https://www.jmir.org/2020/5/e19367) |
| Democratic Republic of Congo | [HDX](https://data.humdata.org/dataset/democratic-republic-of-the-congo-coronavirus-covid-19-subnational-cases) | [CC BY][28] |
| Estonia | [Health Board of Estonia](https://www.terviseamet.ee/et/koroonaviirus/avaandmed) | [Open Data](https://www.terviseamet.ee/et/koroonaviirus/avaandmed) |
| Finland | [Finnish institute for health and welfare](https://thl.fi/en/web/thlfi-en) | [CC BY](https://thl.fi/en/web/thlfi-en/statistics/statistical-databases/open-data) |
| France | [data.gouv.fr](https://data.gouv.fr) | [Open License 2.0](https://www.etalab.gouv.fr/licence-ouverte-open-licence) |
| Germany | [Robert Koch Institute](https://npgeo-corona-npgeo-de.hub.arcgis.com/datasets/dd4580c810204019a7b8eb3e0b329dd6_0?page=26) | [Attribution Required](https://www.govdata.de/dl-de/by-2-0) |
| Haiti | [HDX](https://data.humdata.org/dataset/haiti-covid-19-subnational-cases) | [CC-BY][28] |
| Hong Kong | [Hong Kong Department of Health](https://data.gov.hk/en-data/dataset/hk-dh-chpsebcddr-novel-infectious-agent) | [Attribution Required](https://data.gov.hk/en/terms-and-conditions) |
| Israel | [Israel Government Data Portal](https://data.gov.il/dataset/covid-19) | [Attribution Required](https://data.gov.il/terms) |
| Haiti | [HDX](https://data.humdata.org/dataset/haiti-covid-19-subnational-cases) | [CC BY][28] |
| India | [Wikipedia](https://en.wikipedia.org/wiki/Template:2019-20_coronavirus_pandemic_data/India_medical_cases) | [Attribution Required][24] |
| India | [Covid 19 India Organisation](https://www.covid19india.org/) | [CC BY][29] |
| Indonesia | <https://covid19.go.id/peta-sebaran> | Public Domain |
| Italy | [Italy's Department of Civil Protection](https://github.com/pcm-dpc/COVID-19) | [CC BY](https://github.com/pcm-dpc/COVID-19/blob/master/LICENSE) |
| Iraq | [HDX](https://data.humdata.org/dataset/iraq-coronavirus-covid-19-subnational-cases) | [CC BY][28] |
| Japan | <https://github.com/swsoyee/2019-ncov-japan> | [MIT](https://github.com/swsoyee/2019-ncov-japan/blob/master/LICENSE) |
| Japan | <https://github.com/kaz-ogiwara/covid19> | [MIT](https://github.com/kaz-ogiwara/covid19/blob/master/LICENSE) |
| Libya | [HDX](https://data.humdata.org/dataset/libya-coronavirus-covid-19-subnational-cases) | [CC BY][28] |
| Luxembourg | [data.public.lu](https://data.public.lu/fr/datasets/donnees-covid19)| [CC0](https://data.public.lu/fr/datasets/?license=cc-zero) |
| Malaysia | [Wikipedia](https://en.wikipedia.org/wiki/2020_coronavirus_pandemic_in_Malaysia) | [Attribution Required][24] |
| Mexico | [Secretaría de Salud Mexico](https://coronavirus.gob.mx/) | [Attribution Required](https://datos.gob.mx/libreusomx) |
| Netherlands | [RIVM](https://data.rivm.nl/covid-19) | [Public Domain](https://databronnencovid19.nl/Disclaimer) |
| New Zealand | [Ministry of Health](https://www.health.govt.nz/our-work/diseases-and-conditions/covid-19-novel-coronavirus/covid-19-data-and-statistics) | [CC-BY](https://www.health.govt.nz/about-site/copyright) |
| Norway | [COVID19 EU Data](https://github.com/covid19-eu-zh/covid19-eu-data) | [MIT](https://github.com/covid19-eu-zh/covid19-eu-data/issues/57) |
| Pakistan | [Wikipedia](https://en.wikipedia.org/wiki/Template:2019-20_coronavirus_pandemic_data/Pakistan_medical_cases) | [Attribution Required][24] |
| Peru | [Datos Abiertos Peru](https://www.datosabiertos.gob.pe/group/datos-abiertos-de-covid-19) | [ODC BY][31] |
| Philippines | [Philippines Department of Health](http://www.doh.gov.ph/covid19tracker) | [Attribution required](https://drive.google.com/file/d/1LzY2eLzZQdLR9yuoNufGEBN5Ily8ZTdV) |
| Poland | [COVID19 EU Data](https://github.com/covid19-eu-zh/covid19-eu-data) | [MIT](https://github.com/covid19-eu-zh/covid19-eu-data/issues/57) |
| Portugal | [COVID-19: Portugal](https://github.com/carlospramalheira/covid19) | [MIT](https://github.com/carlospramalheira/covid19/blob/master/LICENSE) |
| Romania | <https://github.com/adrianp/covid19romania> | [CC0](https://github.com/adrianp/covid19romania/blob/master/LICENSE) |
| Romania | <https://datelazi.ro/> | [Terms of Service](https://stirioficiale.ro/termeni-si-conditii-de-utilizare) |
| Russia | <https://стопкоронавирус.рф> (via [@jeetiss](https://github.com/jeetiss/covid19-russia) | [CC BY][29] |
| Slovenia | <https://www.gov.si> | [Attribution Required][24] |
| South Africa| [FinMango COVID-19 Data](https://finmango.org/covid) | [CC BY](https://finmango.org/covid) |
| South Korea | [Wikipedia](https://en.wikipedia.org/wiki/Template:2019%E2%80%9320_coronavirus_pandemic_data/South_Korea_medical_cases) | [Attribution Required][24] |
| Spain | [Ministry of Health](https://covid19.isciii.es) | [Attribution required](https://www.mscbs.gob.es/avisoLegal/home.html) |
| Spain (Canary Islands) | [Gobierno de Canarias](https://grafcan1.maps.arcgis.com/apps/opsdashboard/index.html#/156eddd4d6fa4ff1987468d1fd70efb6) | [Attribution required](https://www.gobiernodecanarias.org/principal/avisolegal.html) |
| Spain (Catalonia) | [Dades Obertes Catalunya](https://analisi.transparenciacatalunya.cat/) | [CC0](https://web.gencat.cat/ca/menu-ajuda/ajuda/avis_legal/) |
| Spain (Madrid) | [Datos Abiertos Madrid](https://www.comunidad.madrid/gobierno/datos-abiertos) | [Attribution required](https://www.comunidad.madrid/gobierno/datos-abiertos/reutiliza#condiciones-uso) |
| Sudan | [HDX](https://data.humdata.org/dataset/sudan-coronavirus-covid-19-subnational-cases) | [CC BY][28] |
| Sweden | [Public Health Agency of Sweden](https://www.folkhalsomyndigheten.se/the-public-health-agency-of-sweden/) | Fair Use |
| Switzerland | [OpenZH data](https://open.zh.ch) | [CC BY](https://github.com/openZH/covid_19/blob/master/LICENSE) |
| Taiwan | [Ministry of Health and Welfare](https://data.cdc.gov.tw/en/dataset/agsdctable-day-19cov/resource/3c1e263d-16ec-4d70-b56c-21c9e2171fc7) | [Attribution Required](https://data.gov.tw/license) |
| Thailand | [Ministry of Public Health](https://covid19.th-stat.com/) | Fair Use |
| Ukraine | [National Security and Defense Council of Ukraine](https://covid19.rnbo.gov.ua/) | [CC BY](https://www.kmu.gov.ua/#layout-footer) |
| United Kingdom | <https://github.com/tomwhite/covid-19-uk-data> | [The Unlicense](https://github.com/tomwhite/covid-19-uk-data/blob/master/LICENSE.txt) |
| United Kingdom | <https://coronavirus.data.gov.uk/> | Attribution required, [Open Government Licence v3.0][32] |
| USA | [NYT COVID Dataset](https://github.com/nytimes) | [Attribution required, non-commercial use](https://github.com/nytimes/covid-19-data/blob/master/LICENSE) |
| USA | [COVID Tracking Project](https://covidtracking.com) | [CC BY](https://covidtracking.com/license) |
| USA (Alaska) | [Alaska Department of Health and Social Services](http://dhss.alaska.gov/dph/Epi/id/Pages/COVID-19/default.aspx) |  |
| USA (D.C.) | [Government of the District of Columbia](https://coronavirus.dc.gov/) | [Public Domain](https://dc.gov/node/939602) |
| USA (Delaware) | [Delaware Health and Social Services](https://coronavirus.dc.gov/) | [Public Domain](https://coronavirus.delaware.gov/coronavirus-graphics/) |
| USA (Florida) | [Florida Health](https://floridahealthcovid19.gov/) | [Public Domain](https://www.dms.myflorida.com/support/terms_and_conditions) |
| USA (Indiana) | [Indiana Department of Health](https://hub.mph.in.gov/organization/indiana-state-department-of-health) | [CC BY](hhttp://www.opendefinition.org/licenses/cc-by) |
| USA (Massachusetts) | [MCAD COVID-19 Information & Resource Center](https://www.mass.gov/info-details/covid-19-updates-and-information) | [Public Domain](https://www.mass.gov/terms-of-use-policy) |
| USA (New York) | [New York City Health Department](https://www1.nyc.gov/site/doh/covid/covid-19-data.page) | [Public Domain](https://www1.nyc.gov/home/terms-of-use.page) |
| USA (San Francisco) | [SF Open Data](https://data.sfgov.org/stories/s/dak2-gvuj) | [Public Domain Dedication and License](https://datasf.org/opendata/terms-of-use/#toc8) |
| USA (Texas) | [Texas Department of State Health Services](https://dshs.texas.gov) | [Attribution required](https://dshs.texas.gov/policy/copyright.shtm) |
| USA (Washington) | [Washington State Department of Health](https://www.doh.wa.gov/Emergencies/COVID19/DataDashboard) | [Public Domain](https://www.doh.wa.gov/PrivacyandCopyright) |
| Venezuela | [HDX](https://data.humdata.org/dataset/corona-virus-covid-19-cases-and-deaths-in-venezuela) | [CC BY][28] |

</details>


[7]: https://github.com/GoogleCloudPlatform/covid-19-open-data/blob/main/examples/data_loading.ipynb
[12]: https://open-covid-19.github.io/explorer
[13]: https://kepler.gl/demo/map?mapUrl=https://dl.dropboxusercontent.com/s/cofdctuogawgaru/COVID-19_Dataset.json
[14]: https://www.starlords3k.com/covid19.php
[15]: https://kiksu.net/covid-19/
[18]: https://www.bsg.ox.ac.uk/research/research-projects/oxford-covid-19-government-response-tracker
[19]: https://auditter.info/covid-timeline
[20]: https://www.coronavirusdailytracker.info/
[21]: https://omnimodel.com/
[22]: https://console.cloud.google.com/marketplace/product/bigquery-public-datasets/covid19-open-data
[23]: https://www.wikidata.org/wiki/Wikidata:Licensing
[24]: https://foundation.wikimedia.org/wiki/Terms_of_Use
[28]: https://data.humdata.org/about/license
[29]: http://creativecommons.org/licenses/by/4.0/
[30]: https://reproduction.live/
[31]: http://opendefinition.org/licenses/odc-by/
[32]: https://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/
[33]: https://ec.europa.eu/info/legal-notice_en#copyright-notice
