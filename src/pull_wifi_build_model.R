# pull in wifi data... to then build a sense of the different groups... help to build goals.
# Mon Jan 24 16:02:20 2022 ------------------------------

library(odbc)
library(DBI)
library(glue)
library(tidyverse)
library(lubridate)
library(prophet)

server <- "appsql-prod.database.windows.net"
database = "tangstats"
con <- DBI::dbConnect(odbc::odbc(), 
                      UID = "pdwalker@tangeroutlet.com", # rstudioapi::askForPassword("pdwalker"),
                      Driver="ODBC Driver 17 for SQL Server",
                      Server = server, Database = database,
                      Authentication = "ActiveDirectoryInteractive")

#----- Pull from dashboard data

pull_sql <- glue_sql(
  "
SELECT cc.CustomerID,  - cc.CustomerContactID AS TransactionID,
	- cc.CustomerContactID AS TransactionDetailID,
	cast(cc.ContactDate AS DATE) as TransactionDate,c.CenterName,
	'A' AS Status, coalesce(p.Description, '')  AS DetailDesc,
	'Customer Contact' AS TransactionHistoryType,
	cc.PromoID
	--, cc.Description as promo_desc
FROM tblCustomerContacts cc
INNER JOIN tblCustomerContactTypes cct ON cc.ContactTypeID = cct.CustomerContactTypeID
INNER JOIN tblPromos p ON cc.PromoID = p.PromoID
LEFT JOIN tblcenters c ON cc.centerid = c.centerid

WHERE year(cc.contactDate) >= 2017
	and cc.PromoID <> 0
and cc.promoid in ('23610', '24865') --wifi landing page opt-in
  ",
.con = con)

parm_sql <- dbSendQuery(con, pull_sql)
wifi_pull <- dbFetch(parm_sql)

dbClearResult(parm_sql)

#--- All Dates -------
datetbl <- tibble(
  ds =seq(as.Date("2017-01-01"), as.Date("2022-01-26"), by = "days"),
  filler = 0
)

#------ Clean Traffic -----

CleanWifi <- 
  wifi_pull %>%
  filter(TransactionDate <= Sys.time()) %>% 
  mutate(Date = as.Date(TransactionDate)) %>% 
  filter(!CenterName %in% c("Bromont", "Corporate", "Jeffersonville", "Nags Head", "No Center Assigned", "Ocean City",
                            "Park City", "Terrell", "Williamsburg", "Saint Sauveur")) %>% 
  group_by(Date) %>%
  summarise(uniqLog = n(),
            DistcCust = n_distinct(CustomerID)) %>% 
  ungroup() %>% 
  mutate(All = "Portfolio") %>% 
  full_join(.,
            datetbl,
            by = c("Date" = "ds")) %>% 
  mutate(uniqLog = coalesce(uniqLog, filler),
         DistcCust = coalesce(DistcCust, filler))



CleanWifi %>% 
  ggplot(aes(Date, uniqLog))+
  geom_point(aes(color = ifelse(uniqLog <=0, 'red', 'black')))+
  geom_line()+
  geom_smooth(method = "loess", color="tomato")+
  scale_y_continuous(labels = scales::comma)+
  scale_color_identity()+
  labs(title = "Total Wifi Logins by Day")

CleanWifi %>% 
  ggplot(aes(Date, DistcCust))+
  geom_point(aes(color = ifelse(DistcCust <=0, 'red', 'black')))+
  geom_line()+
  geom_smooth(method = "loess", color="tomato")+
  scale_y_continuous(labels = scales::comma)+
  scale_color_identity()+
  labs(title = "Unique Customer Wifi Logins by Day")

### Looks like we do want to convert 0 to "NA"... probably a data generation issue?...


#-- Build Models --------
# List of dates covid impacted traffic ------
covid <- tibble(
  holiday = 'covid',
  ds =seq(as.Date("2020-03-01"), as.Date("2021-05-01"), by = "days"),
  lower_window = 0,
  upper_window = 1
)

Model1 <-
  CleanWifi %>% 
  filter(Date < as.Date("2022-01-01")) %>%
  rename(ds = Date,
         y = DistcCust) %>% 
  mutate(y = ifelse(y <= 0, NA_real_, y)) %>% 
  select(ds, y) %>% 
  ungroup()

Model2 <-
  CleanWifi %>% 
  filter(Date < as.Date("2020-01-01")) %>%
  rename(ds = Date,
         y = DistcCust) %>% 
  mutate(y = ifelse(y <= 0, NA_real_, y)) %>% 
  select(ds, y) %>% 
  ungroup()

# Build forecast model: Covid date adjusted -------------
m1 <- prophet(holidays = covid)
m1 <- add_country_holidays(m1, country_name = 'US')
m1 <- fit.prophet(m1, Model1)

future1 <- make_future_dataframe(m1, periods = 365)
forecast1 <- predict(m1, future1)
# plot(m1, forecast1)
# prophet_plot_components(m1, forecast1)

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

save(CleanWifi, Model1, Model2,
     m1, future1, forecast1,
     m2, future2, forecast2,
     m3, future3, forecast3,
     m4, future4, forecast4,
     file = here::here("data", "CleanWifi.rds"))



