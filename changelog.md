### v1.2. Release. 09/10/2021
* Integrated [Unified register of debtors](https://data.gov.ua/dataset/506734bf-2480-448c-a2b4-90b6d06df11e) 
* Get ZIP archive
* Unzip resources
* Read and parse huge CSV
* Convert huge CSV to JSON
* Purging temp files
* Added performance metric into the log file: requests execution time

### v1.0. First release. 26/09/2021
* JSON downloading, parsing, and saving into MongoDB
* Full-text search into datasets
* Search results datasets are saving into HTML files
* Integrated [the missing person register](https://data.gov.ua/en/dataset/470196d3-4e7a-46b0-8c0c-883b74ac65f0) and [the wanted person register](https://data.gov.ua/en/dataset/7c51c4a0-104b-4540-a166-e9fc58485c1b)  
* A full-text search could be performed in any of the three languages: Russian, Ukrainian, and English. Results are showing only in English
* Manual refreshing datasets
* Errors handling
* Full logging
* Built-in managing database operations: creating/truncating collections, creating/deleting text indexes
* Showing datasets the last update date