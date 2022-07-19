# Build simple forecasting model to help set potential goals ---
# Mon Jan 24 16:04:34 2022 ------------------------------

library(tidyverse)
# library(modeltime)

load(file = here::here("data", "CleanTraffc.rds"))


Traffc <-
  CleanTraffc %>% 
  mutate(ds = Date) %>% 
  group_by(ds) %>% 
  summarise(y = sum(traffic, na.rm=T)) %>% 
  ungroup()

# install.packages('Rcpp')
# install.packages("prophet")
library(prophet)

m <- prophet(Traffc)
future <- make_future_dataframe(m, periods = 365)
tail(future)


forecast <- predict(m, future)
tail(forecast[c('ds', 'yhat', 'yhat_lower', 'yhat_upper')])


plot(m, forecast)



prophet_plot_components(m, forecast)
