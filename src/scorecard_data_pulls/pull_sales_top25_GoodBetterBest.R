# pull top retailer revenue (sales) - exec sumry
# pulling top 25 and top 100 good/better/best for courtney
# Thu Jan 06 16:30:24 2022 ------------------------------

library(DBI)
library(glue)
library(tidyverse)
library(lubridate)
library(openxlsx)

datesLk <- readxl::read_excel(here::here("input", "DateLookUp.xlsx"))
GBB <- readxl::read_excel(here::here("input", "GoodBetterBest_lkup.xlsx"))


# ----- Sales Report Date

Today <- Sys.Date()
# Today <- as.Date("2022-01-23")

RPTDate <- 
  if(as.numeric(lubridate::day(Today)) < 20){
    Today %m-% months(2) %>% lubridate::ceiling_date(., "month")-1
  } else {
    # statement(s) will execute if the boolean expression is false.
    Today %m-% months(1) %>% lubridate::ceiling_date(., "month")-1
  }

todFilt <- as.numeric(paste0(lubridate::year(RPTDate),lubridate::month(RPTDate),lubridate::day(RPTDate)))

#- Pull data -----------
warehouse <- DBI::dbConnect(odbc::odbc(), dsn = "WAREHOUSESQL")

pull_sql <- glue_sql(
  "
select dimd.fulldate, dimc.CENTERNAME, dimc.BldgGroupName, dimc.[BLDGID], dimt.TangerCatDesc, dimt.TangerSubCatDesc, diml.TenantName, diml.ChainName, diml.leasid 
,dime.PORTID,fas.ISTEMP, fas.IsCurrentlyOpen,fas.ISNONREPORTING, fas.IsOpenForFullYear, diml.zPopUpProgram, lastlsf, 
[CurrentYearSales1M],[CurrentYearSales12M],
cast(iif(lastlsf = 0 or lastlsf is null, 0, [CurrentYearSales1M]/lastlsf) as money) as AdjustSPSF
from warehouse.dbo.FactAdjustedSales fas 
	left join warehouse.dbo.DimLease diml on fas.LeaseKey = diml.LeaseKey
	left join warehouse.dbo.DimCenter dimc on fas.CenterKey = dimc.CenterKey
	left join warehouse.dbo.DimTenant dimt on fas.TenantKey = dimt.TenantKey
	left join warehouse.dbo.DimEntity dime on fas.EntityKey = dime.EntityKey
	left join warehouse.dbo.DimDate dimd on fas.dateKey = dimd.DateKey
--where fas.DateKey > 20181231
where fas.DateKey in (20191231, 20201231, 20211231) 
and lastlsf > 0
order by CENTERNAME, fas.datekey desc, tenantname
  ",
.con = warehouse)

parm_sql <- dbSendQuery(warehouse, pull_sql)
Sales_center <- dbFetch(parm_sql)

dbClearResult(parm_sql)

#------ Clean sales -----

notInc <- c("Charlotte Outlet Center", "Jeffersonville Outlet Center", "Nags Head Outlet Center", "Ocean City Outlet Center",
            "Park City Outlet Center", "St. Sauveur", "Terrell Outlet Center", "Williamsburg Outlet Center")


CleanSales <- 
  Sales_center %>% 
  mutate(fulldate = as.Date(fulldate),
         Year = lubridate::year(fulldate)) %>% 
  rename(GrossSales = CurrentYearSales1M,
         GrossSales_Rolling12 = CurrentYearSales12M,
         Date = fulldate) %>% 
  filter(!BldgGroupName %in% notInc,
         ISTEMP == "N",
         ISNONREPORTING == "N") %>%
  mutate(across(where(is.character), str_trim),
         Center = str_remove_all(BldgGroupName, " Outlet Center| Outlets")) %>% 
  select(-BLDGID) %>% 
  mutate(CommercialCat = case_when(TangerCatDesc %in% c("Apparel", "Accessories") ~ TangerCatDesc,
                                   TRUE ~ "All Other"),
         # CommercialCat = factor(CommercialCat, levels = c("Apparel", "Accessories", "All Other")),
         CommercialSubCat = case_when(TangerSubCatDesc %in% c("Athletic Apparel") ~ "Athletic",
                                      TRUE ~ "All Other SubCat"),
         All = "Portfolio") %>% 
  left_join(.,
            GBB,
            by = c("TenantName" = "Brand")) %>% 
  mutate(Categories = ifelse(CommercialSubCat == "Athletic", "Athletic", CommercialCat)) %>% 
  select(Date, Year, Center, GrossSales, GrossSales_Rolling12, TenantName,leasid, ChainName, GBB, Categories) 


#---Top 25: Athletic, Apparel, Accessories----------


RptTopCurr <-
  CleanSales %>%
  group_by(Year, TenantName, Categories) %>%
  summarise(Gross1M = sum(GrossSales, na.rm=T),
            Gross12M = sum(GrossSales_Rolling12, na.rm=T),
            UniqLeas = n_distinct(leasid)) %>%   
  filter(Categories != "All Other",
         Year == 2021) %>% 
  group_by(Year) %>%
  arrange(-Gross12M, TenantName) %>%  
  mutate(rank = row_number(),
         rank = as.numeric(rank)) %>% 
  filter(rank <= 25) %>% 
  ungroup()

RptTop <-
  CleanSales %>%
  group_by(Year, TenantName, Categories) %>%
  summarise(Gross1M = sum(GrossSales, na.rm=T),
            Gross12M = sum(GrossSales_Rolling12, na.rm=T),
            UniqLeas = n_distinct(leasid)) %>%   
  ungroup() %>% 
  mutate(PercSales1M = Gross1M/sum(Gross1M, na.rm=T),
         PercSales12M = Gross12M/sum(Gross12M, na.rm=T)) %>% 
  ungroup()%>% 
  filter(TenantName %in% RptTopCurr$TenantName)%>% 
  right_join(., RptTopCurr %>% select(TenantName, rank),
             by = c("TenantName")) %>% 
  ungroup()


RankTop25 <-
  RptTop %>% 
  # select(-Date, -ChainName) %>% 
  pivot_wider(names_from = Year, values_from = c(TenantName, Gross1M, Gross12M, UniqLeas, PercSales1M, PercSales12M)) %>% 
  arrange(rank) %>% 
  select(rank, Categories, 
         TenantName_2021, Gross1M_2021,Gross12M_2021, UniqLeas_2021, PercSales1M_2021, PercSales12M_2021,
         TenantName_2020, Gross1M_2020,Gross12M_2020, UniqLeas_2020, PercSales1M_2020, PercSales12M_2020,
         TenantName_2019, Gross1M_2019,Gross12M_2019, UniqLeas_2019, PercSales1M_2019, PercSales12M_2019)  



#---Top 100: Good, Better Best----------

RptGBBCurr <-
  CleanSales %>%
  filter(!is.na(GBB),
         Year == 2021) %>% 
  group_by(Year, TenantName, GBB) %>%
  summarise(Gross1M = sum(GrossSales, na.rm=T),
            Gross12M = sum(GrossSales_Rolling12, na.rm=T),
            UniqLeas = n_distinct(leasid)) %>%   
  ungroup()%>% 
  group_by(Year) %>%
  arrange(-Gross12M, TenantName) %>%  
  mutate(rank = row_number(),
         rank = as.numeric(rank)) %>% 
  filter(rank <= 100) %>% 
  ungroup()

RptGBB <-
  CleanSales %>%
  group_by(Year, TenantName, GBB) %>%
  summarise(Gross1M = sum(GrossSales, na.rm=T),
            Gross12M = sum(GrossSales_Rolling12, na.rm=T),
            UniqLeas = n_distinct(leasid)) %>%   
  ungroup() %>% 
  mutate(PercSales1M = Gross1M/sum(Gross1M, na.rm=T),
         PercSales12M = Gross12M/sum(Gross12M, na.rm=T)) %>% 
  ungroup()%>% 
  filter(TenantName %in% RptGBBCurr$TenantName)%>% 
  right_join(., RptGBBCurr %>% select(TenantName, rank),
             by = c("TenantName")) %>% 
  ungroup()


RankGBB <-
  RptGBB %>% 
  # select(-Date, -ChainName) %>% 
  pivot_wider(names_from = Year, values_from = c(TenantName, Gross1M, Gross12M, UniqLeas,PercSales1M, PercSales12M)) %>% 
  arrange(rank) %>% 
  select(rank, GBB, 
         TenantName_2021, Gross1M_2021,Gross12M_2021, UniqLeas_2021, PercSales1M_2021, PercSales12M_2021,
         TenantName_2020, Gross1M_2020,Gross12M_2020, UniqLeas_2020, PercSales1M_2020, PercSales12M_2020,
         TenantName_2019, Gross1M_2019,Gross12M_2019, UniqLeas_2019, PercSales1M_2019, PercSales12M_2019)    



#---Top 25: Chain----------

RptChainCurr <-
  CleanSales %>%
  filter(Year == 2021) %>% 
  group_by(Year, ChainName) %>%
  summarise(Gross1M = sum(GrossSales, na.rm=T),
            Gross12M = sum(GrossSales_Rolling12, na.rm=T),
            UniqLeas = n_distinct(leasid)) %>%   
  ungroup()%>% 
  group_by(Year) %>%
  arrange(-Gross12M, ChainName) %>%  
  mutate(rank = row_number(),
         rank = as.numeric(rank)) %>% 
  filter(rank <= 25) %>% 
  ungroup()

RptChain <-
  CleanSales %>%

  group_by(Year, ChainName) %>%
  summarise(Gross1M = sum(GrossSales, na.rm=T),
            Gross12M = sum(GrossSales_Rolling12, na.rm=T),
            UniqLeas = n_distinct(leasid)) %>%   
  ungroup() %>% 
  mutate(PercSales1M = Gross1M/sum(Gross1M, na.rm=T),
         PercSales12M = Gross12M/sum(Gross12M, na.rm=T)) %>% 
  ungroup()%>% 
  filter(ChainName %in% RptChainCurr$ChainName)%>% 
  right_join(., RptChainCurr %>% select(ChainName, rank),
             by = c("ChainName")) %>% 
  ungroup()


RankChain <-
  RptChain %>% 
  # select(-Date, -ChainName) %>% 
  pivot_wider(names_from = Year, values_from = c( Gross1M, Gross12M, UniqLeas, PercSales1M, PercSales12M)) %>% 
  arrange(rank) %>% 
  select(rank, ChainName, 
         Gross1M_2021,Gross12M_2021, UniqLeas_2021, PercSales1M_2021, PercSales12M_2021,
         Gross1M_2020,Gross12M_2020, UniqLeas_2020, PercSales1M_2020, PercSales12M_2020,
         Gross1M_2019,Gross12M_2019, UniqLeas_2019, PercSales1M_2019, PercSales12M_2019)   




# --- Save in Excel


wb <- createWorkbook()
addWorksheet(wb, sheetName = "Top25_Retailer")
addWorksheet(wb, sheetName = "GoodBetterBest")
addWorksheet(wb, sheetName = "Top25_Chain")


# Add Formatting to Spreadsheet
s <- createStyle(numFmt = "$ #,##0")
per <- createStyle(numFmt="0.00%")

addStyle(wb = wb, sheet = 1, style = s, rows = 2:1000, cols = c(4,5, 10,11, 16,17), gridExpand = TRUE)
addStyle(wb = wb, sheet = 2, style = s, rows = 2:1000, cols = c(4,5, 10,11, 16,17), gridExpand = TRUE)
addStyle(wb = wb, sheet = 3, style = s, rows = 2:1000, cols = c(3,4, 8,9, 13,14), gridExpand = TRUE)

addStyle(wb = wb, sheet = 1, style = per, rows = 2:1000, cols = c(7,8, 13, 14, 19, 20), gridExpand = TRUE)
addStyle(wb = wb, sheet = 2, style = per, rows = 2:1000, cols = c(7,8, 13, 14, 19, 20), gridExpand = TRUE)
addStyle(wb = wb, sheet = 3, style = per, rows = 2:1000, cols = c(6,7,11,12,16,17), gridExpand = TRUE)



writeDataTable(wb, sheet = 1, x = RankTop25,
               colNames = TRUE, rowNames = F,
               tableStyle = "TableStyleLight9")
writeDataTable(wb, sheet = 2, x = RankGBB,
               colNames = TRUE, rowNames = F,
               tableStyle = "TableStyleLight9")
writeDataTable(wb, sheet = 3, x = RankChain,
               colNames = TRUE, rowNames = F,
               tableStyle = "TableStyleLight9")

saveWorkbook(wb, file = here::here("output","TopRetailers_25_GoodBetterBest.xlsx"), overwrite = TRUE)


