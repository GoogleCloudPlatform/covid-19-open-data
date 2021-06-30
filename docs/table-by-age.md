[Back to main page](../README.md)

# By Age
Epidemiology and hospitalizations data stratified by age.

Values in this table are stratified versions of the columns available in the
[epidemiology](#epidemiology) and [hospitalizatons](#hospitalizations) tables. Each row contains up
to 10 distinct bins, for example:
`{new_deceased_age_00: 1, new_deceased_age_01: 45, ... , new_deceased_age_09: 32}`.

Each row may have different bins, depending on the data source. This table tries to capture the raw
data with as much fidelity as possible up to 10 bins. The range of each bin is encoded into the
`age_bin_${index}` variable, for example:
`{age_bin_00: 0-9, age_bin_01: 10-19, age_bin_02: 20-29, ... , age_bin_09: 90-}`.

Several things worth noting about this table:
* This table contains very sparse data, with very few combinations of regions and variables
  available.
* Records without a known age bin are discarded, so the sum of all ages may not necessary amount to
  the variable from the corresponding table.
* The upper and lower range of the range are inclusive values. For example, range `0-9` includes
  individuals with age zero up to (and including) 9.
* A row may have less than 10 bins, but never more than 10. For example:
  `{age_bin_00: 0-49, age_bin_01: 50-, age_bin_02: null, ...}`


## URL
This table can be found at the following URLs depending on the choice of format:
* [by-age.csv](https://storage.googleapis.com/covid19-open-data/v2/by-age.csv)
* [by-age.json](https://storage.googleapis.com/covid19-open-data/v2/by-age.json)


## Schema
| Name | Type | Description | Example |
| ---- | ---- | ----------- | ------- |
| **date** | `string` | ISO 8601 date (YYYY-MM-DD) of the datapoint | 2020-03-30 |
| **key** | `string` | Unique string identifying the region | FR |
| **`${statistic}`\_age\_bin\_`${index}`** | `integer` | Value of `${statistic}` for age bin `${index}` | 139 |
| **age\_bin\_`${index}`** | `integer` | Range for the age values inside of bin `${index}`, both ends inclusive | 30-39 |


## Sources of data

<details>
<summary>Show data sources</summary>


| Data | Source | License and Terms of Use |
| ---- | ------ | ------------------------ |
| Argentina | [Datos Argentina](https://datos.gob.ar/dataset/salud-covid-19-casos-registrados-republica-argentina) | [Public domain](https://datos.gob.ar/acerca/seccion/marco-legal) |
| Brazil | [Brazil Ministério da Saúde](https://coronavirus.saude.gov.br/) | [Creative Commons Atribuição](http://www.opendefinition.org/licenses/cc-by) |
| Brazil (Rio de Janeiro) | <http://www.data.rio/> | [Dados abertos](https://www.data.rio/datasets/f314453b3a55434ea8c8e8caaa2d8db5) |
| Brazil (Ceará) | <https://saude.ce.gov.br> | [Dados abertos](https://cearatransparente.ce.gov.br/portal-da-transparencia) |
| Colombia | [Datos Abiertos Colombia](https://www.datos.gov.co) | [Attribution required](https://herramientas.datos.gov.co/es/terms-and-conditions-es) |
| Czech Republic | [Ministry of Health of the Czech Republic](https://onemocneni-aktualne.mzcr.cz/covid-19) | [Open Data](https://www.jmir.org/2020/5/e19367) |
| Estonia | [Health Board of Estonia](https://www.terviseamet.ee/et/koroonaviirus/avaandmed) | [Open Data](https://www.terviseamet.ee/et/koroonaviirus/avaandmed) |
| Finland | [Finnish institute for health and welfare](https://thl.fi/en/web/thlfi-en) | [CC BY](https://thl.fi/en/web/thlfi-en/statistics/statistical-databases/open-data) |
| France | [data.gouv.fr](https://data.gouv.fr) | [Open License 2.0](https://www.etalab.gouv.fr/licence-ouverte-open-licence) |
| Germany | [Robert Koch Institute](https://npgeo-corona-npgeo-de.hub.arcgis.com/datasets/dd4580c810204019a7b8eb3e0b329dd6_0?page=26) | [Attribution Required](https://www.govdata.de/dl-de/by-2-0) |
| Hong Kong | [Hong Kong Department of Health](https://data.gov.hk/en-data/dataset/hk-dh-chpsebcddr-novel-infectious-agent) | [Attribution Required](https://data.gov.hk/en/terms-and-conditions) |
| India | [Covid 19 India Organisation](https://www.covid19india.org/) | [CC BY][29] |
| Mexico | [Secretaría de Salud Mexico](https://coronavirus.gob.mx/) | [Attribution Required](https://datos.gob.mx/libreusomx) |
| New Zealand | [Ministry of Health](https://www.health.govt.nz/our-work/diseases-and-conditions/covid-19-novel-coronavirus/covid-19-data-and-statistics) | [CC-BY](https://www.health.govt.nz/about-site/copyright) |
| Peru | [Datos Abiertos Peru](https://www.datosabiertos.gob.pe/group/datos-abiertos-de-covid-19) | [ODC BY][31] |
| Philippines | [Philippines Department of Health](http://www.doh.gov.ph/covid19tracker) | [Attribution required](https://drive.google.com/file/d/1LzY2eLzZQdLR9yuoNufGEBN5Ily8ZTdV) |
| Romania | <https://datelazi.ro/> | [Terms of Service](https://stirioficiale.ro/termeni-si-conditii-de-utilizare) |
| Spain | [Government Authority](https://covid19.isciii.es) | [Attribution required](https://www.mscbs.gob.es/avisoLegal/home.html) |
| Spain (Canary Islands) | [Gobierno de Canarias](https://grafcan1.maps.arcgis.com/apps/opsdashboard/index.html#/156eddd4d6fa4ff1987468d1fd70efb6) | [Attribution required](https://www.gobiernodecanarias.org/principal/avisolegal.html) |
| Spain (Catalonia) | [Dades Obertes Catalunya](https://analisi.transparenciacatalunya.cat/) | [CC0](https://web.gencat.cat/ca/menu-ajuda/ajuda/avis_legal/) |
| Spain (Madrid) | [Datos Abiertos Madrid](https://www.comunidad.madrid/gobierno/datos-abiertos) | [Attribution required](https://www.comunidad.madrid/gobierno/datos-abiertos/reutiliza#condiciones-uso) |
| Taiwan | [Ministry of Health and Welfare](https://data.cdc.gov.tw/en/dataset/agsdctable-day-19cov/resource/3c1e263d-16ec-4d70-b56c-21c9e2171fc7) | [Attribution Required](https://data.gov.tw/license) |
| Thailand | [Ministry of Public Health](https://covid19.th-stat.com/) | Fair Use |
| USA | [Imperial College of London](https://github.com/ImperialCollegeLondon/US-covid19-agespecific-mortality-data) | [CC BY](https://github.com/ImperialCollegeLondon/US-covid19-agespecific-mortality-data/blob/master/LICENSE) |
| USA (California) | [California Open Data Portal](https://data.ca.gov/dataset/590188d5-8545-4c93-a9a0-e230f0db7290/) | [CC0](https://data.ca.gov/dataset/590188d5-8545-4c93-a9a0-e230f0db7290/) |
| USA (D.C.) | [Government of the District of Columbia](https://coronavirus.dc.gov/) | [Public Domain](https://dc.gov/node/939602) |
| USA (Delaware) | [Delaware Health and Social Services](https://coronavirus.dc.gov/) | [Public Domain](https://coronavirus.delaware.gov/coronavirus-graphics/) |
| USA (Florida) | [Florida Health](https://floridahealthcovid19.gov/) | [Public Domain](https://www.dms.myflorida.com/support/terms_and_conditions) |
| USA (Georgia) | [Georgia Department of Public Health](https://dph.georgia.gov/) | [Fair Use](https://dph.georgia.gov/about-dph/mission-and-values) |
| USA (Indiana) | [Indiana Department of Health](https://hub.mph.in.gov/organization/indiana-state-department-of-health) | [CC BY](hhttp://www.opendefinition.org/licenses/cc-by) |
| USA (Massachusetts) | [MCAD COVID-19 Information & Resource Center](https://www.mass.gov/info-details/covid-19-updates-and-information) | [Public Domain](https://www.mass.gov/terms-of-use-policy) |
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
