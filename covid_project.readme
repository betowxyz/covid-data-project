
# COVID Data Project

## In this project, I'll use an open API to request COVID data and store in a local mysql database. In the first step, I created the "covid_build" file, that create all mysql tables / databases and request & load the data to this tables. The second step will be an ETL using AirFlow to schedule and retrieve the data all days at 00:00:00, and store in the mysql. The third step is to build some data viz. The fourth and last step is to build sobe basic ML models to deep analysis (maybe regressions, classifications, etc).
## You can test all of this code, the only thing that you have to setup and run is the mysql. Other dependencies will be added in a requirements.txt later.

### Database Settings
#### Creation raw_data
CREATE TABLE raw_data (
    `id` int NOT NULL AUTO_INCREMENT,
    `country` VARCHAR(255) NOT NULL,
    `country_code` VARCHAR(4) NOT NULL,
    `lat` DOUBLE NOT NULL,
    `lon` DOUBLE NOT NULL,
    `confirmed` BIGINT,
    `deaths` BIGINT, 
    `recovered` BIGINT,
    `active` BIGINT,
    `date` DATE,
    PRIMARY KEY(`id`)
)

#### Creation countries
CREATE TABLE countries (
    `id` int NOT NULL AUTO_INCREMENT,
    `country` VARCHAR(255) NOT NULL,
    `slug` VARCHAR(255) NOT NULL,
    `iso2` VARCHAR(4) NOT NULL,
    PRIMARY KEY(`id`)
)
