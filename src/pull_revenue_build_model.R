# pull in traffic data... to then build a sense of the different groups... help to build goals.
# Mon Jan 24 16:02:20 2022 ------------------------------


library(odbc)
library(DBI)
library(glue)
library(tidyverse)
library(lubridate)
library(prophet)
# centerLk <- readxl::read_excel(here::here("input", "center_lkup.xlsx"), "center_lkup") %>% 
#   select(CENTER_ID, BldgGroupName, Group, Region, RelativeSize)

#----- Pull from warehouse data
warehouse <- DBI::dbConnect(odbc::odbc(), dsn = "WAREHOUSESQL")

pull_sql <- glue_sql(
  "
select dimd.fulldate, dimc.CENTERNAME, dimc.BldgGroupName, dimc.[BLDGID], dimt.TangerCatDesc, dimt.TangerSubCatDesc, diml.TenantName, diml.ChainName, diml.leasid 
,dime.PORTID,fas.ISTEMP, fas.IsCurrentlyOpen,fas.ISNONREPORTING, fas.IsOpenForFullYear, diml.zPopUpProgram, lastlsf, [CurrentYearSales1M],
cast(iif(lastlsf = 0 or lastlsf is null, 0, [CurrentYearSales1M]/lastlsf) as money) as AdjustSPSF
from warehouse.dbo.FactAdjustedSales fas 
	left join warehouse.dbo.DimLease diml on fas.LeaseKey = diml.LeaseKey
	left join warehouse.dbo.DimCenter dimc on fas.CenterKey = dimc.CenterKey
	left join warehouse.dbo.DimTenant dimt on fas.TenantKey = dimt.TenantKey
	left join warehouse.dbo.DimEntity dime on fas.EntityKey = dime.EntityKey
	left join warehouse.dbo.DimDate dimd on fas.dateKey = dimd.DateKey
where fas.DateKey > 20151231
--and lastlsf > 0
order by CENTERNAME, fas.datekey desc, tenantname
  ",
.con = warehouse)

parm_sql <- dbSendQuery(warehouse, pull_sql)
Sales_center <- dbFetch(parm_sql)

dbClearResult(parm_sql)


#------ Clean Traffic -----

CleanSales <- 
  Sales_center %>% 
  mutate(fulldate = as.Date(fulldate),
         fulldate = lubridate::floor_date(fulldate, "month")) %>% 
  rename(GrossSales = CurrentYearSales1M,
         Date = fulldate) %>% 
  filter(!BLDGID %in% c("BROMO1", "WILLI1", "OCEAN1", "PARKC1", "TERRE1", "STSAU1", "JEFFE1", "CHLTT1", "NAGSH1", "WBROO1")) %>%
  mutate(across(where(is.character), str_trim)) %>% 
  select(-BLDGID) %>% 
  mutate(CommercialCat = case_when(TangerCatDesc %in% c("Apparel", "Accessories") ~ TangerCatDesc,
                                   TRUE ~ "All Other"),
         CommercialCat = factor(CommercialCat, levels = c("Apparel", "Accessories", "All Other")),
         CommercialSubCat = case_when(TangerSubCatDesc %in% c("Athletic Apparel") ~ "Athletic",
                                      TRUE ~ "All Other SubCat"),
         All = "Portfolio") %>% 
  group_by(All, Date) %>% 
  summarise(sales = sum(GrossSales, na.rm=T)) %>% 
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
  CleanSales %>% 
  filter(Date < as.Date("2022-01-01")) %>%
  mutate(ds = Date) %>% 
  group_by(ds) %>% 
  summarise(y = sum(sales, na.rm=T)) %>% 
  ungroup()

Model2 <-
  CleanSales %>% 
  filter(Date < as.Date("2020-01-01")) %>%
  mutate(ds = Date) %>% 
  group_by(ds) %>% 
  summarise(y = sum(sales, na.rm=T)) %>% 
  ungroup()

# Build forecast model: Covid date adjusted -------------
m1 <- prophet(holidays = covid)
m1 <- add_country_holidays(m1, country_name = 'US')
m1 <- fit.prophet(m1, Model1)

future1 <- make_future_dataframe(m1, periods = 24, freq = 'month')
forecast1 <- predict(m1, future1)

# plot(m1, forecast1)
# prophet_plot_components(m1, forecast1)

# Build forecast model2: Level trend -------------
m2 <- prophet(holidays = covid, growth = "flat")
m2 <- add_country_holidays(m2, country_name = 'US')
m2 <- fit.prophet(m2, Model1)

future2 <- make_future_dataframe(m1, periods = 24, freq = 'month')
forecast2 <- predict(m2, future2)

# Build forecast model3: 2020 removed -------------
m3 <- prophet()
m3 <- add_country_holidays(m3, country_name = 'US')
m3 <- fit.prophet(m3, Model2)

future3 <- make_future_dataframe(m1, periods = 12*3, freq = 'month')
forecast3 <- predict(m3, future3)

plot(m3, forecast3)
prophet_plot_components(m3, forecast3)

# Build forecast model4: 2020 removed, level trend-------------
m4 <- prophet(growth = "flat")
m4 <- add_country_holidays(m4, country_name = 'US')
m4 <- fit.prophet(m4, Model2)

future4 <- make_future_dataframe(m1, periods = 12*3, freq = 'month')
forecast4 <- predict(m4, future4)










#---- Save all the objects to call in markdown doc ---------

save(CleanSales, Model1, Model2,
     m1, future1, forecast1,
     m2, future2, forecast2,
     m3, future3, forecast3,
     m4, future4, forecast4,
     file = here::here("data", "CleanSales.rds"))



