### v1.8. Release. 31/10/2021
* Performance optimization
* Refactoring

### v1.7. Release. 28/10/2021
* **[Dockerizing](https://hub.docker.com/repository/docker/andruxa17/searchmydata)**
* **Switched to Python 3.10.0**
* Added setup script for run in Docker
* Added the run script
* Added requirements.txt

### v1.6. Release. 27/10/2021
* Big refactoring and code optimization

### v1.4. Release. 16/10/2021
* Integrated [Unified State Register of Legal Entities, Individual Entrepreneurs and Public Associations](https://data.gov.ua/dataset/1c7f3815-3259-45e0-bdf1-64dca07ddc10)
* Full parsing XML (**finally!**)
* Save parsed XML into MongoDB as a set of JSON
* Clear the results' folder before the search
* Try to use [Garbage Collection](https://docs.python.org/3/library/gc.html) to decrease RAM consumption
* **Fixed**: the app was crashing when a search result can't fit RAM limitations due to by default aggregation in MongoDB occurs in memory and pipeline stages have a limit of 100 Mb RAM. Use "allowDiskUse" 

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