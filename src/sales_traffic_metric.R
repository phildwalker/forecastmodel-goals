# Sales vs Traffic Metric -----
# Sun Jan 30 12:29:25 2022 ------------------------------
library(odbc)
library(DBI)
library(glue)
library(tidyverse)
library(lubridate)
library(prophet)


centerLk <- readxl::read_excel(here::here("input", "center_lkup.xlsx"), "center_lkup") %>% 
  select(CENTER_ID, BldgGroupName, Group, Region, RelativeSize)
#----------------

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


#--------------
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

#----------- Combined


Comb <-
  CleanTraffc %>% 
  mutate(Month = lubridate::floor_date(Date, "month")) %>% 
  group_by(Month) %>% 
  summarise(TotalTraff = sum(traffic, na.rm=T)) %>% 
  ungroup() %>% 
  full_join(.,
            CleanSales %>% select(Date, sales),
            by = c("Month" = "Date")) %>% 
  ungroup() %>% 
  mutate(SalesPerCar = sales/TotalTraff) %>% 
  write_csv(., here::here("output", "SalesTraffic_Comb.csv"))

theme_set(theme_bw())
cb_palette <- c("#003399", "#ff2b4f", "#3686d3", "#fcab27", "#88298a", "#000000", "#969696", "#008f05", "#FF5733", "#E5F828")

Comb %>% 
  ggplot(aes(Month, SalesPerCar))+
  geom_point(aes(color = ifelse(SalesPerCar <=0, 'red', 'black')))+
  geom_line()+
  geom_smooth(method = "loess", color="tomato")+
  scale_y_continuous(labels = scales::dollar)+
  scale_color_identity()+
  labs(title = "Retailer Sales Vs Car by Month")















