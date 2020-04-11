
##Build Status

Overall: [![CircleCI](https://circleci.com/gh/dexterchan/TreeQuery.svg?style=svg)](https://circleci.com/gh/dexterchan/TreeQuery) <br>
Master: [![CircleCI](https://circleci.com/gh/dexterchan/TreeQuery/tree/master.svg?style=svg)](https://circleci.com/gh/dexterchan/TreeQuery/tree/master) <br>


##SQL schema
````
create table DailyGovBondPrice(
	AsOfDate varchar(12),
    Tenor varchar(10),
    Price decimal(10,2),
    primary key(AsOfDate, Tenor)
)
````

###Upload to maven repository
``````
gradlew -Pnexus uploadArchives
``````