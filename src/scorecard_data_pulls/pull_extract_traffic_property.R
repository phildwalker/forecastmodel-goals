# Pull Extract Traffic for exec summary
# Thu Jan 06 18:45:56 2022 ------------------------------


library(odbc)
library(DBI)
library(glue)
library(tidyverse)
library(lubridate)

centerLk <- readxl::read_excel(here::here("input", "center_lkup.xlsx"), "center_lkup") %>% 
  select(CENTER_ID, BldgGroupName, Group, Region, RelativeSize)

datesLk <- readxl::read_excel(here::here("input", "DateLookUp.xlsx"))

#----- Pull from dashboard data

OpsStats <- DBI::dbConnect(odbc::odbc(), dsn = "OpsStats") #"OpsStats-UAT"

pull_sql <- glue_sql(
  "
with traff as (
select countdate, trafficcount as traffic, trafficcount as vehicleTraffic, 0 as peopletraffic, centerid, conditions, modifiedon, closurestart, closureend, closurereason
FROM [OpsStats].[dbo].[tblTraffic] 
where year(countDate) >= '2019'
and countDate < GetDate() -1
and ModifiedOn is not null
and CenterID not in (104) -- removed foxwoods data because shouldn't be captured in this table
UNION ALL

select countdate, trafficcount/3 as traffic, 0 as vehicleTraffic, trafficCount as peopletraffic, centerid, conditions, modifiedon, NULL as closurestart, NULL as closureend, '' as closurereason
FROM [OpsStats].[dbo].[tblPeopleTraffic] 
where year(countDate) >= '2019'
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
  filter(!BldgGroupName %in% c("Foley Outlet Center", "Pittsburgh Outlet Center", "National Harbor Outlet Center", 
                               "Foxwoods Outlet Center")) %>% # remove non comp centers
  ungroup() 



# CleanTraffc %>% 
#   filter(is.na(RelativeSize)) %>% 
#   distinct(BldgGroupName)

#------ Build Comp function ----------

BuildCompAll <- function(GroupCol, DateGroup){
  col_name <- enquo(GroupCol)
  sel_var <- as.name(DateGroup)
  
  dat <- 
    CleanTraffc %>% 
    fuzzyjoin::fuzzy_left_join(.,
                    datesLk %>% mutate(Start = as.Date(Start), End = as.Date(End)) %>% filter(Type %in% DateGroup),
                    by = c("Date" = "Start",
                           "Date" = "End"),
                    match_fun = list(`>=`, `<=`)) %>% 
    filter(!is.na(Year)) %>% 
    mutate(Year = as.character(Year)) %>% 
    group_by(Type, Year,DateOrder, AggregGrouping = !!col_name) %>% 
    summarise(gross = sum(traffic, na.rm=T),
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


bind_rows(
  CleanTraffc %>% 
  fuzzyjoin::fuzzy_left_join(.,
                             datesLk %>% mutate(Start = as.Date(Start), End = as.Date(End)) %>% filter(Type %in% c("YTD")),
                             by = c("Date" = "Start",
                                    "Date" = "End"),
                             match_fun = list(`>=`, `<=`)) %>% 
  filter(!is.na(Year),
         BldgGroupName %in% c("Myrtle Beach 17 Outlet Center")) %>%
  mutate(Year = as.character(Year)) %>% 
  group_by(Type, Year,DateOrder, AggregGrouping = BldgGroupName) %>% 
  summarise(gross = sum(traffic, na.rm=T),
            MinDate = min(Date),
            MaxDate = max(Date)) %>% 
  ungroup() %>% 
  mutate(Group = as.character(glue::glue("[{MinDate} - {MaxDate}]"))) %>% 
  select(-MinDate, -MaxDate, -Type, dates=Group) %>% #, "{{sel_var}}_gross" := gross, "{{sel_var}}_dates" := Group
  arrange(AggregGrouping, Year) %>% 
  # mutate(Type = DateGroup) %>% 
  pivot_wider(names_from = DateOrder, values_from= c(gross, dates, Year)) %>% 
  mutate(PerComp_CY_LY = (gross_CY/gross_LY)-1,
         PerComp_CY_LY2 = (gross_CY/gross_LY2)-1),
  CleanTraffc %>% 
    fuzzyjoin::fuzzy_left_join(.,
                               datesLk %>% mutate(Start = as.Date(Start), End = as.Date(End)) %>% filter(Type %in% c("Wk")),
                               by = c("Date" = "Start",
                                      "Date" = "End"),
                               match_fun = list(`>=`, `<=`)) %>% 
    filter(!is.na(Year),
           BldgGroupName %in% c("Myrtle Beach 17 Outlet Center")) %>%
    mutate(Year = as.character(Year)) %>% 
    group_by(Type, Year,DateOrder, AggregGrouping = BldgGroupName) %>% 
    summarise(gross = sum(traffic, na.rm=T),
              MinDate = min(Date),
              MaxDate = max(Date)) %>% 
    ungroup() %>% 
    mutate(Group = as.character(glue::glue("[{MinDate} - {MaxDate}]"))) %>% 
    select(-MinDate, -MaxDate, -Type, dates=Group) %>% #, "{{sel_var}}_gross" := gross, "{{sel_var}}_dates" := Group
    arrange(AggregGrouping, Year) %>% 
    # mutate(Type = DateGroup) %>% 
    pivot_wider(names_from = DateOrder, values_from= c(gross, dates, Year)) %>% 
    mutate(PerComp_CY_LY = (gross_CY/gross_LY)-1,
           PerComp_CY_LY2 = (gross_CY/gross_LY2)-1)
) %>% 
  write_cs




#------ Gather Data --------


results <-
  bind_rows(
    # -- last week
    BuildCompAll(GroupCol = All, DateGroup = "Wk"),
    BuildCompAll(GroupCol = Region, DateGroup = "Wk"),
    BuildCompAll(GroupCol = RelativeSize, DateGroup = "Wk"),
    # -- mtd
    BuildCompAll(GroupCol = All, DateGroup = "MTD"),
    BuildCompAll(GroupCol = Region, DateGroup = "MTD"),
    BuildCompAll(GroupCol = RelativeSize, DateGroup = "MTD"),
    # -- ytd
    BuildCompAll(GroupCol = All, DateGroup = "YTD"),
    BuildCompAll(GroupCol = Region, DateGroup = "YTD"),
    BuildCompAll(GroupCol = RelativeSize, DateGroup = "YTD"),
    # -- certif mtd
    BuildCompAll(GroupCol = All, DateGroup = "Lck-MTD"),
    BuildCompAll(GroupCol = Region, DateGroup = "Lck-MTD"),
    BuildCompAll(GroupCol = RelativeSize, DateGroup = "Lck-MTD"),
    # -- certif mtd
    BuildCompAll(GroupCol = All, DateGroup = "Lck-YTD"),
    BuildCompAll(GroupCol = Region, DateGroup = "Lck-YTD"),
    BuildCompAll(GroupCol = RelativeSize, DateGroup = "Lck-YTD"),
  )


results %>% 
  mutate(lookup= as.character(glue::glue("{AggregGrouping}|{Type}"))) %>% 
  relocate(lookup) %>% 
  write_csv(., file = here::here("output", "Traffic_Scorecard_Results.csv"))



















# DateGroup = "Wk"
# sel_var <- as.name(DateGroup)
# 
# CleanTraffc %>% 
#   fuzzyjoin::fuzzy_left_join(.,
#                              datesLk %>% mutate(Start = as.Date(Start), End = as.Date(End)) %>% filter(Type %in% "Wk"),
#                              by = c("Date" = "Start",
#                                     "Date" = "End"),
#                              match_fun = list(`>=`, `<=`)) %>% 
#   filter(!is.na(Year)) %>% 
#   mutate(Year = as.character(Year)) %>% 
#   group_by(Type, Year,DateOrder, AggregGrouping = Region) %>% 
#   summarise(gross = sum(traffic, na.rm=T),
#             MinDate = min(Date),
#             MaxDate = max(Date)) %>% 
#   ungroup() %>% 
#   mutate(Group = as.character(glue::glue("[{MinDate} - {MaxDate}]"))) %>% 
#   select(-MinDate, -MaxDate, -Type, dates=Group) %>% #, "{{sel_var}}_gross" := gross, "{{sel_var}}_dates" := Group
#   arrange(AggregGrouping, Year) %>% 
#   mutate(Type = DateGroup) %>% 
#   pivot_wider(names_from = DateOrder, values_from= c(gross, dates, Year)) %>% 
#   mutate(PerComp_CY_LY = (gross_CY/gross_LY)-1,
#          PerComp_CY_LY2 = (gross_CY/gross_LY2)-1)





