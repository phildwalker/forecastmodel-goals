# pull in traffic data... to then build a sense of the different groups... help to build goals.
# Mon Jan 24 16:02:20 2022 ------------------------------

library(odbc)
library(DBI)
library(glue)
library(tidyverse)
library(lubridate)
library(prophet)


centerLk <- readxl::read_excel(here::here("input", "center_lkup.xlsx"), "center_lkup") %>% 
  select(CENTER_ID, BldgGroupName, Group, Region, RelativeSize)

# datesLk <- readxl::read_excel(here::here("input", "DateLookUp.xlsx"))

#----- Pull from dashboard data

OpsStats <- DBI::dbConnect(odbc::odbc(), dsn = "OpsStats") #"OpsStats-UAT"

pull_sql <- glue_sql(
  "
with traff as (
select countdate, trafficcount as traffic, trafficcount as vehicleTraffic, 0 as peopletraffic, centerid, conditions, modifiedon, closurestart, closureend, closurereason
FROM [OpsStats].[dbo].[tblTraffic] 
where year(countDate) >= '2016'
and countDate < GetDate() -1
and ModifiedOn is not null
and CenterID not in (104) -- removed foxwoods data because shouldn't be captured in this table
UNION ALL

select countdate, trafficcount/3 as traffic, 0 as vehicleTraffic, trafficCount as peopletraffic, centerid, conditions, modifiedon, NULL as closurestart, NULL as closureend, '' as closurereason
FROM [OpsStats].[dbo].[tblPeopleTraffic] 
where year(countDate) >= '2016'
and countDate < GetDate() -1
and centerid = 104 --Only pull foxwoods people traffic
)

select t.*, c.entityid, c.description as CenterLocation, c.closed, c.traffictype
FROM traff t left join
   OpsStats.dbo.tblCenter C on t.CenterID = C.CenterID
where c.active = 1
--and t.centerid in (6, 104) --6: Gonzales, 104: Foxwoods
  --and TrafficCount > 0
order by t.centerid, countdate desc

  ",
.con = OpsStats)

parm_sql <- dbSendQuery(OpsStats, pull_sql)
traffic <- dbFetch(parm_sql)

dbClearResult(parm_sql)


#------ Clean Traffic -----

CleanTraffc <- 
  traffic %>% 
  left_join(., 
            centerLk, by = c("entityid" = "CENTER_ID")) %>% 
  filter(!is.na(BldgGroupName)) %>% 
  mutate(Date = as.Date(countdate),
         month = lubridate::ceiling_date(Date, "month")-1) %>% 
  filter(!BldgGroupName %in% c("Atlantic City Outlet Center")) %>% #, "Ottawa Outlet Center", "Cookstown"
  ungroup() %>% 
  select(Date, traffic, BldgGroupName, Group, Region, RelativeSize) %>% 
  mutate(All = "Portfolio") %>% 
  # filter(!BldgGroupName %in% c("Foley Outlet Center", "Pittsburgh Outlet Center", "National Harbor Outlet Center", 
  #                              "Foxwoods Outlet Center")) %>% # remove non comp centers
  ungroup() 
#-- Build Models --------
# List of dates covid impacted traffic ------
covid <- tibble(
  holiday = 'covid',
  ds =seq(as.Date("2020-03-01"), as.Date("2021-05-01"), by = "days"),
  lower_window = 0,
  upper_window = 1
)

Model1 <-
  CleanTraffc %>% 
  filter(Date < as.Date("2022-01-01")) %>%
  mutate(ds = Date) %>% 
  group_by(ds) %>% 
  summarise(y = sum(traffic, na.rm=T)) %>% 
  ungroup()

Model2 <-
  CleanTraffc %>% 
  filter(Date < as.Date("2020-01-01")) %>%
  mutate(ds = Date) %>% 
  group_by(ds) %>% 
  summarise(y = sum(traffic, na.rm=T)) %>% 
  ungroup()

# Build forecast model: Covid date adjusted -------------
m1 <- prophet(holidays = covid)
m1 <- add_country_holidays(m1, country_name = 'US')
m1 <- fit.prophet(m1, Model1)

future1 <- make_future_dataframe(m1, periods = 365)
forecast1 <- predict(m1, future1)

# Build forecast model2: Level trend -------------
m2 <- prophet(holidays = covid, growth = "flat")
m2 <- add_country_holidays(m2, country_name = 'US')
m2 <- fit.prophet(m2, Model1)

future2 <- make_future_dataframe(m2, periods = 365)
forecast2 <- predict(m2, future2)

# Build forecast model3: 2020 removed -------------
m3 <- prophet()
m3 <- add_country_holidays(m3, country_name = 'US')
m3 <- fit.prophet(m3, Model2)

future3 <- make_future_dataframe(m3, periods = 365*3)
forecast3 <- predict(m3, future3)

# Build forecast model4: 2020 removed, level trend-------------
m4 <- prophet(growth = "flat")
m4 <- add_country_holidays(m4, country_name = 'US')
m4 <- fit.prophet(m4, Model2)

future4 <- make_future_dataframe(m4, periods = 365*3)
forecast4 <- predict(m4, future4)










#---- Save all the objects to call in markdown doc ---------

save(CleanTraffc, Model1, Model2,
     m1, future1, forecast1,
     m2, future2, forecast2,
     m3, future3, forecast3,
     m4, future4, forecast4,
     file = here::here("data", "CleanTraffc.rds"))



