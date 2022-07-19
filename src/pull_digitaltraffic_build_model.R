# pull in traffic data... to then build a sense of the different groups... help to build goals.
# Mon Jan 24 16:02:20 2022 ------------------------------


library(tidyverse)
library(lubridate)
library(prophet)


#----- Pull from dashboard data
digTraff <- read_csv(here::here("input", "DigitalTraffic_Raw.csv"))

#------ Clean Traffic -----

CleanTraffc %>% 
  summarise(MinDt = min(Date, na.rm=T),
            MaxDt = max(Date, na.rm=T))

datetbl <- tibble(
  ds =seq(as.Date("2009-04-02"), as.Date("2022-01-26"), by = "days"),
  filler = 0
)

CleanTraffc <- 
  digTraff %>% 
  mutate(Date = as.Date(`Day Index`, format = "%m/%d/%Y")) %>% 
  ungroup() %>% 
  select(`Day Index`, Date, Sessions) %>% 
  filter(!is.na(Date)) %>% 
  mutate(All = "Portfolio") %>% 
  ungroup()


CleanTraffc %>% 
  ggplot(aes(Date, Sessions))+
  geom_point()+
  geom_line()+
  geom_smooth(method = "loess", color="tomato")+
  scale_y_continuous(labels = scales::comma)+
  labs(title = "Web Sessions by Day")

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
  summarise(y = sum(Sessions, na.rm=T)) %>% 
  ungroup()

Model2 <-
  CleanTraffc %>% 
  filter(Date < as.Date("2020-01-01")) %>%
  mutate(ds = Date) %>% 
  group_by(ds) %>% 
  summarise(y = sum(Sessions, na.rm=T)) %>% 
  ungroup()

# Build forecast model: Covid date adjusted -------------
m1 <- prophet(holidays = covid, changepoint.prior.scale = 0.2)
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
# m4 <- prophet(growth = "flat")
# m4 <- add_country_holidays(m4, country_name = 'US')
# m4 <- fit.prophet(m4, Model2)
# 
# future4 <- make_future_dataframe(m4, periods = 365*3)
# forecast4 <- predict(m4, future4)










#---- Save all the objects to call in markdown doc ---------

save(CleanTraffc, Model1, Model2,
     m1, future1, forecast1,
     m2, future2, forecast2,
     m3, future3, forecast3,
     file = here::here("data", "CleanDigitalTraffc.rds"))



