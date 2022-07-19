# pull in receipts data... to then build a sense of the different groups... help to build goals.
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
Select CustomerID, CenterID, TentID, CreatedOn, PurchaseDate, ReceiptAmount, MinsBtwnPurchAndSubmit 
	From (
		Select * 
			, datediff(minute, PurchaseDate, CreatedOn) as MinsBtwnPurchAndSubmit
		From tblReceiptTracking 
		Where Status = 'A'
			And DeletedOn is null 
			And TentID != 'TCPTS'
			And TentID is not null 
			And TentID != 'OTHER'
			And ReceiptAmount < 100000
	) a

  ",
.con = con)

parm_sql <- dbSendQuery(con, pull_sql)
recpts <- dbFetch(parm_sql)

dbClearResult(parm_sql)

#--- All Dates -------
datetbl <- tibble(
  ds =seq(as.Date("2017-01-01"), as.Date("2022-01-26"), by = "days"),
  filler = 0
)

#------ Clean Traffic -----

CleanRecpts <-
  recpts %>%
  mutate(Date = as.Date(PurchaseDate),
         PuchMo = lubridate::floor_date(Date, "month"),
         CreatDate = as.Date(CreatedOn),
         CreaMo = lubridate::floor_date(CreatDate, "month"),
         Within24 = ifelse(MinsBtwnPurchAndSubmit < (60*24), T, F),
         WithinMo = ifelse(PuchMo == CreaMo, T, F)) %>% 
  filter(WithinMo == T) %>% 
  group_by(CreatDate) %>%
  summarise(TotalLogged = n(),
            UniqCust = n_distinct(CustomerID),
            TotalDollars = sum(ReceiptAmount, na.rm=T)) %>% 
  ungroup() %>% 
  mutate(All = "Portfolio") %>% 
  # full_join(.,
  #           datetbl,
  #           by = c("Date" = "ds")) %>%
  mutate(TotalLogged = coalesce(TotalLogged, 0),
         UniqCust = coalesce(UniqCust, 0),
         TotalDollars = coalesce(TotalDollars, 0)) %>%
    ungroup() %>% 
  rename(Date = CreatDate)



CleanRecpts %>% 
  ggplot(aes(Date, TotalLogged))+
  geom_point(aes(color = ifelse(TotalLogged <=0, 'red', 'black')))+
  geom_line()+
  geom_smooth(method = "loess", color="tomato")+
  scale_y_continuous(labels = scales::comma)+
  scale_color_identity()+
  labs(title = "Total Logged by Day")

CleanRecpts %>% 
  ggplot(aes(Date, UniqCust))+
  geom_point(aes(color = ifelse(UniqCust <=0, 'red', 'black')))+
  geom_line()+
  geom_smooth(method = "loess", color="tomato")+
  scale_y_continuous(labels = scales::comma)+
  scale_color_identity()+
  labs(title = "Unique Customer by Day")


CleanRecpts %>% 
  ggplot(aes(Date, TotalDollars))+
  geom_point(aes(color = ifelse(TotalDollars <=0, 'red', 'black')))+
  geom_line()+
  geom_smooth(method = "loess", color="tomato")+
  scale_y_continuous(labels = scales::comma)+
  scale_color_identity()+
  labs(title = "Total Dollars by Day")

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
  CleanRecpts %>% 
  filter(Date >= as.Date("2015-01-01"),
         Date < as.Date("2022-01-01")) %>%
  rename(ds = Date,
         y = UniqCust) %>% 
  mutate(y = ifelse(y <= 0, NA_real_, y)) %>% 
  select(ds, y) %>% 
  ungroup()

Model2 <-
  CleanRecpts %>% 
  filter(Date >= as.Date("2015-01-01"),
         Date < as.Date("2020-01-01")) %>%
  rename(ds = Date,
         y = UniqCust) %>% 
  mutate(y = ifelse(y <= 0, NA_real_, y)) %>% 
  select(ds, y) %>% 
  ungroup()

# Build forecast model: Covid date adjusted -------------
m1 <- prophet(holidays = covid)
m1 <- add_country_holidays(m1, country_name = 'US')
m1 <- fit.prophet(m1, Model1)

future1 <- make_future_dataframe(m1, periods = 365)
forecast1 <- predict(m1, future1)

plot(m1, forecast1)
prophet_plot_components(m1, forecast1)

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

save(CleanRecpts, Model1, Model2,
     m1, future1, forecast1,
     m2, future2, forecast2,
     m3, future3, forecast3,
     m4, future4, forecast4,
     file = here::here("data", "CleanRecpts.rds"))



