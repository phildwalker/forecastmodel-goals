# pull top retailer revenue (sales) - exec sumry
# Thu Jan 06 16:30:24 2022 ------------------------------

library(DBI)
library(glue)
library(tidyverse)
library(lubridate)

datesLk <- readxl::read_excel(here::here("input", "DateLookUp.xlsx"))

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


# --------------------
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
where fas.DateKey > 20181231
--and lastlsf > 0
order by CENTERNAME, fas.datekey desc, tenantname
  ",
.con = warehouse)

parm_sql <- dbSendQuery(warehouse, pull_sql)
Sales_center <- dbFetch(parm_sql)

dbClearResult(parm_sql)

#------ Clean sales -----

CleanSales <- 
  Sales_center %>% 
  mutate(fulldate = as.Date(fulldate)) %>% 
  rename(GrossSales = CurrentYearSales1M,
         Date = fulldate) %>% 
  filter(Date <= RPTDate,
         !BLDGID %in% c("BROMO1", "WILLI1", "OCEAN1", "PARKC1", "TERRE1", "STSAU1", "JEFFE1", "CHLTT1", "NAGSH1", "WBROO1")) %>%
  mutate(across(where(is.character), str_trim)) %>% 
  select(-BLDGID) %>% 
  mutate(CommercialCat = case_when(TangerCatDesc %in% c("Apparel", "Accessories") ~ TangerCatDesc,
                                   TRUE ~ "All Other"),
         CommercialCat = factor(CommercialCat, levels = c("Apparel", "Accessories", "All Other")),
         CommercialSubCat = case_when(TangerSubCatDesc %in% c("Athletic Apparel") ~ "Athletic",
                                      TRUE ~ "All Other SubCat"),
         All = "Portfolio")

# --------- Large LSF

largeTENANTS <- 
  CleanSales %>% 
  filter(lastlsf >= 20000) %>% 
  distinct(CENTERNAME, TenantName)


#----- Date Selection -------

MaxDate <- 
  CleanSales %>% 
  count(Date) %>%  
  filter(Date == max(Date)) %>%
  pull(Date)

# Lck_minQTR <- lubridate::floor_date(RPTDate, "quarter")
# LY_Lck_minQTR <- minQTR %m+% years(-1)

Lck_minYR <- lubridate::floor_date(RPTDate, "year")
LY_Lck_RPTDate <- RPTDate %m+% years(-1)
LY_Lck_minYR <- Lck_minYR %m+% years(-1)

# ------- Top N - Report Month -------------

# RptTop <-
#   CleanSales %>% 
#   filter(Date == RPTDate) %>% 
#   group_by(TangerCatDesc, ChainName) %>% 
#   summarise(TotalGross = sum(GrossSales, na.rm=T),
#             UniqLeas = n_distinct(leasid),
#             UnqBrands = n_distinct(TenantName)) %>% 
#   ungroup() %>% 
#   arrange(-TotalGross, ChainName) %>% 
#   mutate(rank = row_number(),
#          rank = as.numeric(rank),
#          PercAll = TotalGross/ sum(TotalGross))
# 
# 
# TopN <- 
#   RptTop %>%
#   filter(rank <= 10) 

#------ Build Comps functions----  

# BuildCompAll <- function(fullds = TRUE, GroupCol = c(All, CommericalCat, CommercialSubCat), StartDate, EndDate, LY_Start, LY_End, TypeName){
#   if(fullds == TRUE){
#     CleanSales <- CleanSales
#   } else {
#     CleanSales <- CleanSales %>% filter(ChainName %in% TopN$ChainName)
#   }
#   
#   col_name <- enquo(GroupCol)
#   
#   dat <- 
#     CleanSales %>% 
#     mutate(DateGroup = case_when(Date <= StartDate & Date >= EndDate ~ "CurReport",
#                                  Date <= LY_Start & Date >= LY_End ~ "LY_CurReport",
#                                  TRUE ~ NA_character_)) %>% 
#     filter(!is.na(DateGroup)) %>% 
#     group_by(DateGroup, AggregGrouping = !!col_name) %>% 
#     summarise(gross = sum(GrossSales, na.rm=T),
#               MinDate = min(Date),
#               MaxDate = max(Date)) %>% 
#     ungroup() %>% 
#     mutate(Group = as.character(glue::glue("[{MinDate} - {MaxDate}]"))) %>%
#     select(AggregGrouping, DateGroup, gross, Group) %>% 
#     pivot_wider(names_from = DateGroup, values_from = c(gross, Group)) %>% 
#     mutate(Type = TypeName, 
#            PercComp = (gross_CurReport - gross_LY_CurReport)/gross_LY_CurReport)
#   
#   
#   return(dat)
# } 

# DateGroup <- "Lck-MTD"

BuildComp <- function(GroupCol, DateGroup){
  col_name <- enquo(GroupCol)
  sel_var <- as.name(DateGroup)
  
  dat <- 
    CleanSales %>% 
    fuzzyjoin::fuzzy_left_join(.,
                               datesLk %>% mutate(Start = as.Date(Start), End = as.Date(End)) %>% filter(Type %in% DateGroup),
                               by = c("Date" = "Start",
                                      "Date" = "End"),
                               match_fun = list(`>=`, `<=`)) %>% 
    filter(!is.na(Year)) %>% 
    mutate(Year = as.character(Year)) %>% 
    group_by(Type, Year,DateOrder, AggregGrouping = !!col_name) %>% #!!col_name
    summarise(gross = sum(GrossSales, na.rm=T),
              MinDate = min(Date),
              MaxDate = max(Date)) %>% 
    ungroup() %>% 
    mutate(Group = as.character(glue::glue("[{MinDate} - {MaxDate}]"))) %>% 
    select(-MinDate, -MaxDate, -Type, dates=Group) %>% #, "{{sel_var}}_gross" := gross, "{{sel_var}}_dates" := Group
    arrange(AggregGrouping, Year) %>% 
    mutate(Type = DateGroup) %>% 
    pivot_wider(names_from = DateOrder, values_from= c(gross, dates, Year)) %>% 
    mutate(PerComp_CY_LY = (gross_CY/gross_LY)-1,
           PerComp_CY_LY2 = (gross_CY/gross_LY2)-1)
  
  
  return(dat)
} 

#------ Generate Datasets----  

results <-
  bind_rows(
    # -- certif mtd
    BuildComp(GroupCol = All, DateGroup = "Lck-MTD"),
    # -- certif mtd
    BuildComp(GroupCol = All, DateGroup = "Lck-YTD"),
    # -- certif mtd
    BuildComp(GroupCol = CommercialCat, DateGroup = "Lck-MTD"),
    # -- certif mtd
    BuildComp(GroupCol = CommercialCat, DateGroup = "Lck-YTD"),
    # -- certif mtd
    BuildComp(GroupCol = CommercialSubCat, DateGroup = "Lck-MTD"),
    # -- certif mtd
    BuildComp(GroupCol = CommercialSubCat, DateGroup = "Lck-YTD")
  ) %>% 
  # arrange(AggregGrouping, Type) %>% 
  ungroup()


results %>% 
  mutate(lookup= as.character(glue::glue("{AggregGrouping}|{Type}"))) %>% 
  relocate(lookup) %>% 
  write_csv(., file = here::here("output", "retailerRev_Scorecard_Results.csv"))












# bind_rows(BuildCompAll(fullds = FALSE, GroupCol = All, 
#                        StartDate = RPTDate, EndDate = RPTDate, LY_Start = LY_Lck_RPTDate, LY_End = LY_Lck_RPTDate, TypeName = "MTD_TopN"), 
#           BuildCompAll(fullds = FALSE, GroupCol = All,
#                        StartDate = RPTDate, EndDate = Lck_minYR, LY_Start = LY_Lck_RPTDate, LY_End = LY_Lck_minYR, TypeName = "YTD_TopN"))  %>% 
#   write_csv(., file = here::here("output", "SalesCompar_TopN.csv"))
# 
# 
# bind_rows(BuildCompAll(fullds = TRUE, GroupCol = All, 
#                        StartDate = RPTDate, EndDate = RPTDate, LY_Start = LY_Lck_RPTDate, LY_End = LY_Lck_RPTDate, TypeName = "MTD"), 
#           BuildCompAll(fullds = TRUE, GroupCol = All,
#                        StartDate = RPTDate, EndDate = Lck_minYR, LY_Start = LY_Lck_RPTDate, LY_End = LY_Lck_minYR, TypeName = "YTD"))  %>% 
#   write_csv(., file = here::here("output", "SalesCompar_All.csv"))
# 
# 
# bind_rows(BuildCompAll(fullds = TRUE, GroupCol = CommercialCat, 
#                        StartDate = RPTDate, EndDate = RPTDate, LY_Start = LY_Lck_RPTDate, LY_End = LY_Lck_RPTDate, TypeName = "MTD"), 
#           BuildCompAll(fullds = TRUE, GroupCol = CommercialCat,
#                        StartDate = RPTDate, EndDate = Lck_minYR, LY_Start = LY_Lck_RPTDate, LY_End = LY_Lck_minYR, TypeName = "YTD"),
#           BuildCompAll(fullds = TRUE, GroupCol = CommercialSubCat, 
#                        StartDate = RPTDate, EndDate = RPTDate, LY_Start = LY_Lck_RPTDate, LY_End = LY_Lck_RPTDate, TypeName = "MTD"), 
#           BuildCompAll(fullds = TRUE, GroupCol = CommercialSubCat,
#                        StartDate = RPTDate, EndDate = Lck_minYR, LY_Start = LY_Lck_RPTDate, LY_End = LY_Lck_minYR, TypeName = "YTD"))  %>%
#   filter(!AggregGrouping %in% c("All Other SubCat")) %>% 
#   mutate(AggregGrouping = factor(AggregGrouping, levels= c("Apparel", "Athletic", "Accessories", "All Other"))) %>% 
#   arrange(AggregGrouping) %>% 
#   write_csv(., file = here::here("output", "SalesCompar_Categ.csv"))


#--------------


# test <-
  CleanSales %>% 
  distinct(ChainName, TenantName, TangerCatDesc, TangerSubCatDesc) %>% 
  write_csv(., here::here("output", "Brand_MRI_Category.csv"))


