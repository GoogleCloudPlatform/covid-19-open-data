[Back to main page](../README.md)

# Vaccinations
Information related to deployment and administration of COVID-19 vaccines.

## URL
This table can be found at the following URLs depending on the choice of format:
* [vaccinations.csv](https://storage.googleapis.com/covid19-open-data/v2/vaccinations.csv)
* [vaccinations.json](https://storage.googleapis.com/covid19-open-data/v2/vaccinations.json)

## Schema
| Name | Type | Description | Example |
| ---- | ---- | ----------- | ------- |
| **date** | `string` | ISO 8601 date (YYYY-MM-DD) of the datapoint | 2021-02-07 |
| **key** | `string` | Unique string identifying the region | ID |
| **new_persons_vaccinated\*** | `integer` | Count of new persons which have received one or more doses | 7222 |
| **total_persons_vaccinated\*\*** | `integer` | Cumulative sum of persons which have received one or more doses | 784318 |
| **new_persons_fully_vaccinated\*** | `integer` | Count of new persons which have received all doses required for maximum immunity | 1924 |
| **total_persons_fully_vaccinated\*\*** | `integer` | Cumulative sum of persons which have received all doses required for maximum immunity | 139131 |
| **new_vaccine_doses_administered\*** | `integer` | Count of new vaccine doses administered to persons | 9146 |
| **total_vaccine_doses_administered\*\*** | `integer` | Cumulative sum of vaccine doses administered to persons | 923449 |

\*Values can be negative, typically indicating a correction or an adjustment in the way they were
measured.

\*\*Total count will not always amount to the sum of daily counts, because many authorities make
changes to criteria for counting cases, but not always make adjustments to the data. There is also
potential missing data. All of that makes the total counts *drift* away from the sum of all daily
counts over time, which is why the cumulative values, if reported, are kept in a separate column.


## Sources of data

<details>
<summary>Show data sources</summary>


| Data | Source | License and Terms of Use | Notes |
| ---- | ------ | ------------------------ | ----- |
| Country-level data | [Our World in Data](https://ourworldindata.org) | [CC BY](https://ourworldindata.org/how-to-use-our-world-in-data#how-is-our-work-copyrighted) | |
| Argentina | [Datos Argentina](https://datos.gob.ar/dataset/salud-vacunas-contra-covid-19-dosis-aplicadas-republica-argentina---registro-desagregado) | [Public domain](https://datos.gob.ar/acerca/seccion/marco-legal) |
| Australia | [COVID LIVE](https://covidlive.com.au/) | [CC BY](https://creativecommons.org/licenses/by/4.0/) | Country level data is not the sum of the states/territories as there is a portion of vaccinations managed by the Federal government that is delivered directly to aged and disability care and not counted as part of the states/territories.<br/><br/>As of 2021-03-14, only doses administered are reported for country-level data but NSW, VIC and WA continue to report the count of persons fully and partially vaccinated. |
| Austria | [Open Data Österreich](https://www.data.gv.at/covid-19/) | [CC BY](https://www.data.gv.at/covid-19/) | |
| Belgium | [Covid Vaccinations Belgium](https://covid-vaccinatie.be/en) | [CC BY](https://covid-vaccinatie.be/api) | Regional data only available for Brussels, since the regions reported by the data source do not match our indexed subregions |
| Bolivia | [Ministry of Health](https://www.minsalud.gob.bo/) (via [FinMango][1]) | [CC BY](https://finmango.org/covid) | |
| Brazil | Secretarias de Saúde via [FinMango][1] | [CC BY][1] | |
| Brazil | [Brazil Ministério da Saúde](https://coronavirus.saude.gov.br/) | [Creative Commons Atribuição](http://www.opendefinition.org/licenses/cc-by) | |
| Bulgaria | [Ministry of Health](https://coronavirus.bg/bg/statistika) (via [FinMango][1]) | [CC BY](https://finmango.org/covid) | |
| Canada | [Department of Health Canada](https://www.canada.ca/en/public-health) | [Attribution required](https://www.canada.ca/en/transparency/terms.html) | |
| Colombia | [Ministry of Health](https://www.minsalud.gov.co/salud/publica/Vacunacion/Paginas/Vacunacion-covid-19.aspx) (via [FinMango][1]) | [CC BY](https://finmango.org/covid) | |
| Czech Republic | [Ministry of Health of the Czech Republic](https://onemocneni-aktualne.mzcr.cz/covid-19) | [Open Data](https://www.jmir.org/2020/5/e19367) | |
| France | [data.gouv.fr](https://www.data.gouv.fr/fr/datasets/donnees-relatives-aux-personnes-vaccinees-contre-la-covid-19-1/) | [Open License 2.0](https://www.etalab.gouv.fr/licence-ouverte-open-licence) | |
| Germany | [Robert Koch Institute](https://www.rki.de/DE/Content/InfAZ/N/Neuartiges_Coronavirus/Daten/Impfquoten-Tab.html;jsessionid=7CD5258893F719D9991A9BAEC2B971F0.internet081) (via [FinMango][1]) | [Attribution Required](https://www.govdata.de/dl-de/by-2-0) | |
| India | [COVID19-India](https://github.com/covid19india/api) | [CC BY](https://github.com/covid19india/api/blob/master/LICENSE_DATA) | |
| Israel | [Israel Government Data Portal](https://data.gov.il/dataset/covid-19) | [Attribution Required](https://data.gov.il/terms) | Admin level 2 regions are provided by the source and are aggregated to admin level 1. The total vaccination dose numbers provided by the source for admin level 2 do not match the country-wide total. This also impacts the aggregated level 1 totals. |
| Italy | [Commissario straordinario per l'emergenza Covid-19](https://github.com/italia/covid19-opendata-vaccini) | [CC BY](https://github.com/italia/covid19-opendata-vaccini/blob/master/LICENSE.md) | |
| Spain | [Ministry of Health](https://www.mscbs.gob.es/profesionales/saludPublica/ccayes/alertasActual/nCov/vacunaCovid19.htm) | [Attribution required](https://www.mscbs.gob.es/avisoLegal/home.html) | |
| Slovenia | [Ministry of Health](https://www.nijz.si/sl/cepljenje-proti-covid-19-za-strokovno-javnost) (via [FinMango][1]) | [CC BY](https://finmango.org/covid) | |
| Slovakia | [https://korona.gov.sk](https://korona.gov.sk), operated by Ministry of Investments, Regional Development and Informatization of the Slovak Republic] | [Attribution required](https://www.mirri.gov.sk/en/ministerstvo/legal-information/) | |
| Sweden | [Public Health Agency of Sweden](https://www.folkhalsomyndigheten.se/smittskydd-beredskap/utbrott/aktuella-utbrott/covid-19/vaccination-mot-covid-19/statistik/statistik-over-registrerade-vaccinationer-covid-19/) | Fair Use | |
| Switzerland | [Federal Office of Public Health](https://www.covid19.admin.ch/en/epidemiologic/vacc-doses?detGeo=CH) | [Fair Use](https://www.admin.ch/gov/en/start/terms-and-conditions.html) |
| United Kingdom (nations) | [NHS](https://coronavirus.data.gov.uk/details/vaccinations) | [OGL](http://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/) | |
| United Kingdom (England) | [NHS](https://www.england.nhs.uk/statistics/statistical-work-areas/covid-19-vaccinations/) (via [FinMango][1]) | [OGL](http://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/) | |
| United States  | [CDC](https://covid.cdc.gov/covid-data-tracker/#vaccinations) | [Public Domain](https://www.cdc.gov/other/agencymaterials.html) | |

</details>

[1]: https://finmango.org/covid
