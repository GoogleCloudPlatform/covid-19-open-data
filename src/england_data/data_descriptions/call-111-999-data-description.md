[Back to main page](../README.md)

# Daily 111 and 999 calls
This dataset contains daily numbers of 111 and 999 phone calls which are
associated with possible COVID-19 symptoms, reported by England clinical
commissioning group (CCG), beginning March 18, 2020. The data are collected and
published by the UK government.


## URLs
Historical copies of all raw data is stored here, where the date "20210419" can
be edited to retrieve any date starting from March 18, 2020:
* [https://storage.cloud.google.com/covid19-open-data/dm-c19-data/raw/20210419/calls_111_999.csv](https://storage.cloud.google.com/covid19-open-data/dm-c19-data/raw/20210419/calls_111_999.csv).

All standardized data is stored here, where the date "20210419" can be edited to
retrieve any date starting from March 18, 2020:
* [https://storage.cloud.google.com/covid19-open-data/dm-c19-data/standardized/20210419/calls_111_999.csv](https://storage.cloud.google.com/covid19-open-data/dm-c19-data/standardized/20210419/calls_111_999.csv).

All mergeable data is stored here, where the date "20210419" can be edited to
retrieve any date starting from March 18, 2020:
* [https://storage.cloud.google.com/covid19-open-data/dm-c19-data/mergeable/20210419/calls_111_999.csv](https://storage.cloud.google.com/covid19-open-data/dm-c19-data/mergeable/20210419/calls_111_999.csv).


## Schema
| Name | Type | Description | Example |
| ---- | ---- | ----------- | ------- |
| **date** | `string` | ISO 8601 date (YYYY-MM-DD) of the datapoint | 2020-03-30 |
| **key** | `string` | Unique string identifying the region | GB_ENG_E38000217 |
| **calls_111_f_0** | `integer` | 111 phone calls queries for person classified as female, with age classification 0-18 | 100 |
| **calls_111_f_19** | `integer` | 111 phone calls queries for person classified as female, with age classification 19-69 | 100 |
| **calls_111_f_70** | `integer` | 111 phone calls queries for person classified as female, with age classification 70+ | 100 |
| **calls_111_f_u** | `integer` | 111 phone calls queries for person classified as female, without age classification | 100 |
| **calls_111_m_0** | `integer` | 111 phone calls queries for person classified as male, with age classification 0-18 | 100 |
| **calls_111_m_19** | `integer` | 111 phone calls queries for person classified as male, with age classification 19-69 | 100 |
| **calls_111_m_70** | `integer` | 111 phone calls queries for person classified as male, with age classification 70+ | 100 |
| **calls_111_m_u** | `integer` | 111 phone calls queries for person classified as male, without age classification | 100 |
| **calls_111_u_0** | `integer` | 111 phone calls queries for person without gender classification, with age classification 0-18 | 100 |
| **calls_111_u_19** | `integer` | 111 phone calls queries for person without gender classification, with age classification 19-69 | 100 |
| **calls_111_u_70** | `integer` | 111 phone calls queries for person without gender classification, with age classification 70+ | 100 |
| **calls_111_u_u** | `integer` | 111 phone calls queries for person without gender classification, without age classification | 100 |
| **calls_999_f_0** | `integer` | 999 phone calls queries for person classified as female, with age classification 0-18 | 100 |
| **calls_999_f_19** | `integer` | 999 phone calls queries for person classified as female, with age classification 19-69 | 100 |
| **calls_999_f_70** | `integer` | 999 phone calls queries for person classified as female, with age classification 70+ | 100 |
| **calls_999_f_u** | `integer` | 999 phone calls queries for person classified as female, without age classification | 100 |
| **calls_999_m_0** | `integer` | 999 phone calls queries for person classified as male, with age classification 0-18 | 100 |
| **calls_999_m_19** | `integer` | 999 phone calls queries for person classified as male, with age classification 19-69 | 100 |
| **calls_999_m_70** | `integer` | 999 phone calls queries for person classified as male, with age classification 70+ | 100 |
| **calls_999_m_u** | `integer` | 999 phone calls queries for person classified as male, without age classification | 100 |
| **calls_999_u_0** | `integer` | 999 phone calls queries for person without gender classification, with age classification 0-18 | 100 |
| **calls_999_u_19** | `integer` | 999 phone calls queries for person without gender classification, with age classification 19-69 | 100 |
| **calls_999_u_70** | `integer` | 999 phone calls queries for person without gender classification, with age classification 70+ | 100 |
| **calls_999_u_u** | `integer` | 999 phone calls queries for person without gender classification, without age classification | 100 |


## Sources of data

<details>
<summary>Show data sources</summary>


| Data | Source | License and Terms of Use |
| ---- | ------ | ------------------------ |
| 111 and 999 calls | [111 and 999 calls - latest](https://digital.nhs.uk/data-and-information/publications/statistical/mi-potential-covid-19-symptoms-reported-through-nhs-pathways-and-111-online/latest)
 | [OGL v.3](https://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/) |

</details>
