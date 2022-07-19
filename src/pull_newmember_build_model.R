# pull in new member data... to then build a sense of the different groups... help to build goals.
# Mon Jan 24 16:02:20 2022 ------------------------------

library(odbc)
library(DBI)
library(glue)
library(tidyverse)
library(lubridate)
library(prophet)

today <- Sys.Date()
# today <- as.Date("2022-01-10")

server <- "appsql-prod.database.windows.net"
database = "tangstats"
con <- DBI::dbConnect(odbc::odbc(), 
                      UID = "pdwalker@tangeroutlet.com", # rstudioapi::askForPassword("pdwalker"),
                      Driver="ODBC Driver 17 for SQL Server",
                      Server = server, Database = database,
                      Authentication = "ActiveDirectoryInteractive")


#----- pull data: new members
pull_sql <- glue_sql(
  "
Select cust.CustomerID, cust.CreatedOn as CustCreationDT
	From tblCustomers cust
where cust.CreatedBy <> 2620
order by cust.CustomerID
  ",
.con = con)

parm_sql <- dbSendQuery(con, pull_sql)
newCust <- dbFetch(parm_sql)

dbClearResult(parm_sql)

# save(newCust, file = here::here("data", "newCust.rds"))


#-----------

pull_sql <- glue_sql(
  "
With tc_new_members (TC_CustID, CreatedBy, CenterID,CenterName, FromWeb, TransactionDetailID, TransactionID, ProductID, ForCenterID, 
Quantity, Price, Status, CreatedOn, ModifiedOn, TransactionDate) as 
(
	Select t.CustomerID, t.CreatedBy, t.CenterID,c.CenterName, t.FromWeb, td.TransactionDetailID, td.TransactionID, td.ProductID, 
	 td.ForCenterID, td.Quantity, td.Price, td.Status, cast(td.CreatedOn as date), td.ModifiedOn, cast(t.TransactionDate as date)  
	From tblTransactionDetail td 
	Inner Join tblTransactions t 
		On td.TransactionID = t.TransactionID
	left join tblcenters c ON t.centerid = c.centerid
	Where td.ProductID = 1  --New TC Membership
		And td.DeletedOn is null
		And td.Status in ('A', 'N')
		And t.Status in ('A', 'N')
)

select *
from tc_new_members

  ",
.con = con)

parm_sql <- dbSendQuery(con, pull_sql)
NewMem <- dbFetch(parm_sql)

dbClearResult(parm_sql)


#----- Join datasets together ---------

CentersRemo <- c("NE Prop/N Conway", "McMinnville", "Barstow", "Boaz", "Bourne", "Branson", "Bromont", "Burlington",
                 "Casa Grande", "Corporate", "Dalton", "Jeffersonville", "Kittery", "Lincoln City", "Martinsburg",
                 "McMinnville", "Nags Head", "No Center Assigned", "North Branch", "Ocean City", "Park City",
                 "Pigeon Forge", "Saint Sauveur", "Sanibel", "Seymour", "Stroud", "Terrell", "TEST CENTER", "Tuscola",
                 "Vero Beach", "West Branch", "Westbrook", "Williamsburg", "Wisconsin Dells")


CustMem <- 
  full_join(newCust,
            NewMem,
            by = c("CustomerID" = "TC_CustID")) %>% 
  mutate(All = "Portfolio",
         Date = as.Date(CustCreationDT),
         TCDate = as.Date(TransactionDate),
         TCorInsid = ifelse(is.na(TransactionID), "Insider", "TC"),
         TC_whenJoin = ifelse(as.Date(CustCreationDT) == as.Date(CreatedOn), "StartTC", "StartInsider"),
         WebCent = ifelse(FromWeb == T, "Web", "Center")) %>%
  ungroup() %>% 
  filter(!is.na(TCDate),
         TCDate >= as.Date("2000-01-01"),
         !CenterName %in% CentersRemo) %>% 
  # select(CustomerID, TransactionID, TCDate, WebCent) %>% 
  ungroup()

# test <- CustMem %>% 
#   count(WebCent, CenterName) 


#--- All Dates -------
datetbl <- bind_rows(
  tibble(
    ds =seq(as.Date("2017-01-01"), as.Date("2022-01-26"), by = "days"),
    WebCent = c("Center"),
    filler = 0
    ),
  tibble(
    ds =seq(as.Date("2017-01-01"), as.Date("2022-01-26"), by = "days"),
    WebCent = c("Web"),
    filler = 0
  ),
)

#------ Clean Traffic -----

CleanNewMem <-
  CustMem %>%
  mutate(Date = as.Date(TCDate)) %>% 
  group_by(Date, WebCent) %>%
  summarise(NewMemCount = n()) %>% 
  ungroup() %>% 
  mutate(All = "Portfolio") %>% 
  full_join(.,
            datetbl,
            by = c("Date" = "ds", "WebCent")) %>%
  mutate(NewMemCount = coalesce(NewMemCount, 0)) %>%
  ungroup() 


CleanNewMem %>% 
  filter(Date >= as.Date("2017-01-01")) %>%
  ggplot(aes(Date, NewMemCount))+
  geom_point(aes(color = ifelse(NewMemCount <=0, 'red', 'black')), alpha=0.8)+
  geom_line(alpha=0.6)+
  geom_smooth(method = "loess", color="tomato")+
  scale_y_continuous(labels = scales::comma)+
  scale_color_identity()+
  labs(title = "Total New Members by Source")+
  facet_wrap(WebCent ~ . , ncol = 2, scales = "free_y")


### The days with null new members makes sense. 


#--- looking at the different percentile values for the web data to set a growth cap
CleanNewMem %>% 
  filter(WebCent == "Web") %>% 
    summarise( qnt_0   = quantile(NewMemCount, probs= 0),
            qnt_25  = quantile(NewMemCount, probs= 0.25),
            qnt_50  = quantile(NewMemCount, probs= 0.5),
            qnt_75  = quantile(NewMemCount, probs= 0.75),
            qnt_90  = quantile(NewMemCount, probs= 0.90),
            qnt_95  = quantile(NewMemCount, probs= 0.95),
            qnt_100 = quantile(NewMemCount, probs= 1),
            mean = mean(NewMemCount),
            sd = sd(NewMemCount)
    )  %>% 
  ungroup()
  

#-- Build Models --------
# List of dates covid impacted traffic ------
covid <- tibble(
  holiday = 'covid',
  ds =seq(as.Date("2020-03-01"), as.Date("2021-05-01"), by = "days"),
  lower_window = 0,
  upper_window = 1
)

Model1_Center <-
  CleanNewMem %>% 
  filter(Date >= as.Date("2017-01-01"),
         Date < as.Date("2022-01-01"),
         !WebCent == "Web") %>%
  rename(ds = Date,
         y = NewMemCount) %>% 
  # mutate(y = ifelse(y <= 0, NA_real_, y)) %>% 
  select(WebCent, ds, y) %>% 
  ungroup()

Model2_Center <-
  CleanNewMem %>% 
  filter(Date >= as.Date("2017-01-01"),
         Date < as.Date("2020-01-01"),
         !WebCent == "Web") %>%
  rename(ds = Date,
         y = NewMemCount) %>% 
  # mutate(y = ifelse(y <= 0, NA_real_, y)) %>% 
  select(WebCent, ds, y) %>% 
  ungroup()


Model1_Web <-
  CleanNewMem %>% 
  filter(Date >= as.Date("2017-01-01"),
         Date < as.Date("2022-01-01"),
         WebCent == "Web") %>%
  rename(ds = Date,
         y = NewMemCount) %>% 
  # mutate(y = ifelse(y <= 0, NA_real_, y)) %>% 
  select(WebCent, ds, y) %>% 
  ungroup()

Model2_Web <-
  CleanNewMem %>% 
  filter(Date >= as.Date("2017-01-01"),
         Date < as.Date("2020-01-01"),
         WebCent == "Web") %>%
  rename(ds = Date,
         y = NewMemCount) %>% 
  # mutate(y = ifelse(y <= 0, NA_real_, y)) %>% 
  select(WebCent, ds, y) %>% 
  ungroup()

# Build forecast model: Covid date adjusted // Center // linear growth-------------

Model1_Center$floor <- 20
Model1_Center$cap <- 1000

m1C <- prophet(holidays = covid) #growth = "logistic"
m1C <- add_country_holidays(m1C, country_name = 'US')
m1C <- fit.prophet(m1C, Model1_Center)
future1C <- make_future_dataframe(m1C, periods = 365)

future1C$floor <- 20
future1C$cap <- 1000

forecast1C <- predict(m1C, future1C)

# plot(m1C, forecast1C) + add_changepoints_to_plot(m1C)
# prophet_plot_components(m1, forecast1)

# Build forecast model: Covid date adjusted // Center // logistic growth-------------

Model1_Center$floor <- 20
Model1_Center$cap <- 1000

m2C <- prophet(holidays = covid, growth = "logistic") #growth = "logistic"
m2C <- add_country_holidays(m2C, country_name = 'US')
m2C <- fit.prophet(m2C, Model1_Center)
future2C <- make_future_dataframe(m2C, periods = 365)

future2C$floor <- 20
future2C$cap <- 1000

forecast2C <- predict(m2C, future2C)

plot(m2C, forecast2C) + add_changepoints_to_plot(m2C)
# prophet_plot_components(m2C, forecast2C)

test <-
  forecast2C %>% 
  filter(lubridate::year(ds) == 2022) %>% 
  select(ds, yhat)
#--------WEB  ------------


# Build forecast model: Covid date adjusted // web // linear -------------

Model1_Web$floor <- 20
Model1_Web$cap <- 110 # 95the percentile

m1W <- prophet(holidays = covid) #growth = "logistic"
m1W <- add_country_holidays(m1W, country_name = 'US')
m1W <- fit.prophet(m1W, Model1_Web)
future1W <- make_future_dataframe(m1W, periods = 365)

future1W$floor <- 20
future1W$cap <- 110

forecast1W <- predict(m1W, future1W)

# plot(m1W, forecast1W) + add_changepoints_to_plot(m1W)
# prophet_plot_components(m1, forecast1)

# Build forecast model: Covid date adjusted // web // logistic-------------

Model1_Web$floor <- 20
Model1_Web$cap <- 110

m2W <- prophet(holidays = covid,growth="logistic") #growth = "logistic"
m2W <- add_country_holidays(m2W, country_name = 'US')
m2W <- fit.prophet(m2W, Model1_Web)
future2W <- make_future_dataframe(m2W, periods = 365)

future2W$floor <- 20
future2W$cap <- 110

forecast2W <- predict(m2W, future2W)
# plot(m2W, forecast2W) + add_changepoints_to_plot(m2W)



#--------- Center // Remove Covid // linear growth -----------------


Model2_Center$floor <- 20
Model2_Center$cap <- 1000

m3C <- prophet() #growth = "logistic"
m3C <- add_country_holidays(m3C, country_name = 'US')
m3C <- fit.prophet(m3C, Model2_Center)
future3C <- make_future_dataframe(m3C, periods = 365*3)

future3C$floor <- 20
future3C$cap <- 1000

forecast3C <- predict(m3C, future3C)

# (p <- plot(m3C, forecast3C) + add_changepoints_to_plot(m3C))


#--------- Web // Remove Covid // linear growth -----------------


Model2_Web$floor <- 20
Model2_Web$cap <- 130

m4W <- prophet(growth = "logistic") #growth = "logistic"
m4W <- add_country_holidays(m4W, country_name = 'US')
m4W <- fit.prophet(m4W, Model2_Web)
future4W <- make_future_dataframe(m4W, periods = 365*3)

future4W$floor <- 20
future4W$cap <- 130

forecast4W <- predict(m4W, future4W)

# (p <- plot(m4W, forecast4W) + add_changepoints_to_plot(m4W))


# d1 <- Model1 %>% 
#   nest(data = c(ds, y)) %>% 
#   mutate(m = map(data, ~prophet(holidays = covid) %>% 
#                    add_country_holidays(., country_name = 'US') %>% 
#                    fit.prophet(., Model1))) %>% 
#   mutate(future = map(m, make_future_dataframe, period = 365)) %>% 
#   mutate(forecast = map2(m, future, predict))
# 
# 
# d <- 
#   d1 %>% 
#   unnest(ds, WebCent, yhat)
# 
# plot(m1, forecast1)
# prophet_plot_components(m1, forecast1)

# Build forecast model2: Level trend -------------
# m2 <- prophet(holidays = covid, growth = "flat")
# m2 <- add_country_holidays(m2, country_name = 'US')
# m2 <- fit.prophet(m2, Model1)
# 
# future2 <- make_future_dataframe(m2, periods = 365)
# forecast2 <- predict(m2, future2)
# 
# # Build forecast model3: 2020 removed -------------
# m3 <- prophet()
# m3 <- add_country_holidays(m3, country_name = 'US')
# m3 <- fit.prophet(m3, Model2)
# 
# future3 <- make_future_dataframe(m3, periods = 365*3)
# forecast3 <- predict(m3, future3)
# 
# # Build forecast model4: 2020 removed, level trend-------------
# m4 <- prophet(growth = "flat")
# m4 <- add_country_holidays(m4, country_name = 'US')
# m4 <- fit.prophet(m4, Model2)
# 
# future4 <- make_future_dataframe(m4, periods = 365*3)
# forecast4 <- predict(m4, future4)










#---- Save all the objects to call in markdown doc ---------

save(CleanNewMem, Model1_Center, Model1_Web, Model2_Center, Model2_Web,
     m1C, forecast1C, m2C, forecast2C,
     m1W, forecast1W, m2W, forecast2W,
     m3C, forecast3C,
     m4W, forecast4W,
     file = here::here("data", "CleanNewMem.rds"))



