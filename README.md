
# COVID Data Project

### In this project, I'll use an open API to request COVID data and store in a local mysql database. In the first step, I created the "covid_build" file, that create all mysql tables / databases and request & load the data to this tables. The second step will be an ETL using AirFlow to schedule and retrieve the data all days at 00:00:00, and store in the mysql. The third step is to build some data viz. The fourth and last step is to build sobe basic ML models to deep analysis (maybe regressions, classifications, etc).
#### You can test all of this code, the only thing that you have to setup and run is the mysql. Other dependencies will be added in a requirements.txt later.


### @ToDos
#### Make the connection between the project in docker with mysql works.
#### Schedule to get d-1 data.
#### Data viz.
#### Data analysis.
#### Divide the requests when response equals to "for performance reasons, please specify a province or a date range up to a week", http_status = 400.