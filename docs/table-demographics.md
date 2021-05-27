[Back to main page](../README.md)

# Demographics
Information related to the population demographics for each region.


## URL
This table can be found at the following URLs depending on the choice of format:
* [demographics.csv](https://storage.googleapis.com/covid19-open-data/v3/demographics.csv)
* [demographics.json](https://storage.googleapis.com/covid19-open-data/v3/demographics.json)


## Schema
| Name | Type | Description | Example |
| ---- | ---- | ----------- | ------- |
| **key** | `string` | Unique string identifying the region | KR |
| **population** | `integer` | Total count of humans | 51606633 |
| **population_male** | `integer` | Total count of males | 25846211 |
| **population_female** | `integer` | Total count of females | 25760422 |
| **rural_population** | `integer` | Population in a rural area | 9568386 |
| **urban_population** | `integer` | Population in an urban area | 42038247 |
| **largest_city_population** | `integer` | Population in the largest city of the region | 9963497 |
| **clustered_population** | `integer` | Population in urban agglomerations of more than 1 million | 25893097 |
| **population_density** | `double` `[persons per squared kilometer]` | Population per squared kilometer of land area | 529.3585 |
| **human_development_index** | `double` `[0-1]` | Composite index of life expectancy, education, and per capita income indicators | 0.903 |
| **population_age_`${lower}`_`${upper}`\*** | `integer` | Estimated population between the ages of `${lower}` and `${upper}`, both inclusive | 42038247 |
| **population_age_80_and_older\*** | `integer` | Estimated population over the age of 80 | 477081 |

\*Structured population data is estimated from the WorldPop project. Refer to the
[WorldPop documentation](https://www.worldpop.org/geodata/summary?id=24798) for more details.


## Sources of data

<details>
<summary>Show data sources</summary>


| Data | Source | License and Terms of Use |
| ---- | ------ | ------------------------ |
| Metadata | [Wikipedia](https://wikidata.org) | [Terms of Use][24] |
| Metadata | [Eurostat](https://ec.europa.eu/eurostat) | [CC BY][33] |
| Demographics | [Wikidata](https://wikidata.org) | [CC0][23] |
| Demographics | [UN World Population Prospects](https://population.un.org/wpp/) | [CC BY 3.0 IGO](https://population.un.org/wpp/Download/Standard/CSV/) |
| Demographics | [DataCommons](https://datacommons.org) | [Attribution required](https://policies.google.com/terms) |
| Demographics | [WorldBank](https://worldbank.org) | [CC BY](https://www.worldbank.org/en/about/legal/terms-of-use-for-datasets) |
| Demographics | [WorldPop](https://www.worldpop.org) | [CC BY](https://creativecommons.org/licenses/by/4.0/) |
| Argentina (2010 Census) | [Instituto Nacional de Estadística y Censos](https://www.indec.gob.ar/indec/web/Nivel4-Tema-2-41-135) | [Public domain](https://datos.gob.ar/acerca/seccion/marco-legal) |
| Chile (2017 Census) | [Instituto Nacional de Estadística](https://www.ine.cl/estadisticas/sociales/censos-de-poblacion-y-vivienda/poblacion-y-vivienda) | [CC BY](https://datos.gob.cl/) |
| Israel (2019 Census) | [Central Bureau of Statistics](https://www.cbs.gov.il/he/settlements/Pages/default.aspx?mode=Metropolin) | [Attribution Required](https://www.cbs.gov.il/en/Pages/Enduser-license.aspx) |
| Indonesia (2020 Census) | Central Bureau of Statistics | [Attribution required](https://www.bps.go.id/website/fileMenu/S&K.pdf) |
| Mexico (2010 Census) | [INEGI](https://www.inegi.org.mx/programas/ccpv/2010/default.html) | [Attribution Required](https://datos.gob.mx/libreusomx) |
| Peru (2017 Census) | [INEI](https://censos2017.inei.gob.pe/redatam/) | [ODC BY][31] |
| Philippines (2015 Census) | [Philippine Statistics Authority](https://psa.gov.ph/population-and-housing/statistical-tables) | [Attribution Required](https://psa.gov.ph/article/terms-use) |
| USA (2019 Census) | [United States Census Bureau](https://www.census.gov/data/tables/time-series/demo/popest/2010s-counties-total.html) | [Public Domain](https://ask.census.gov/prweb/PRServletCustom?pyActivity=pyMobileSnapStart&ArticleID=KCP-4726) |

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
