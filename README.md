# Election Markup Language (EML) to csv converter

Election Markup Language ([EML][eml]) is an international standard for storing election data. The Dutch Electoral Council (Kiesraad) uses the EML format to publish election results per candidate, per polling station. 

The script `parse_eml.py` converts EML files from the Kiesraad to csv files.

## Getting started

- Create a project folder with subfolders `data` and `script`
- Download election results [here][data], unzip, and store in the data folder.
- Run `parse_eml.py` from the script folder.
- The csv files will be stored in `data/csv`.

## Caveats

- The script appears to work for Lower House (TK), Provincial Council (PS) and City Council (GR) election results, but not for referendums and Senate (EK) election results.
- I have no idea whether non-Dutch election results in EML format can be parsed with this script.
- Please check the results; they are not guaranteed to be accurate.

[data]:https://data.overheid.nl/data/dataset?maintainer_facet=http%3A%2F%2Fstandaarden.overheid.nl%2Fowms%2Fterms%2FKiesraad
[eml]:https://en.wikipedia.org/wiki/Election_Markup_Language