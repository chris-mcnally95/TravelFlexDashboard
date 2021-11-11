# Assign Connection to Synapse
synapse_server <- "swhscphipprduks01.sql.azuresynapse.net"
synapse_database <- "exploratorydb"
connection_driver <- "{ODBC Driver 17 for SQL Server}"

con <- DBI::dbConnect(odbc::odbc(),
                      driver = connection_driver,
                      database = synapse_database,
                      Authentication="ActiveDirectoryMSI",
                      server = synapse_server)

# Standard SQL Query Function
getTable <- function(table) {
  query <- paste("SELECT * FROM", table)
  data <- DBI::dbGetQuery(con, query)
  message(paste0("Successfully retrieved retrieved data from ", table))
  return(data)
}

# Combined Table Function Using dplyr (requires dbplyr loaded)
getTableFilteredCombined <- function(table1, table2, table3) {
  
  short_locations <- function(x) { 
    dplyr::tbl(con, x) %>% 
      dplyr::filter(CreatedOn >= '20210104',
                    TypeOfPlace == "Travel outside Northern Ireland") %>% 
      dplyr::select(CollectCallId,
                    TypeOfPlace,
                    CountriesVisited,
                    WhenDidYouLeaveNorthernIreland,
                    WhenDidYouReturnToNorthernIreland,
                    AdditionalTravelInformation,
                    MoreDetail)
  }
  
  short_collectcontactcalls <- function(x) {
    dplyr::tbl(con, x) %>% 
      dplyr::filter(CreatedOn >= '20210104') %>%  
      dplyr::select(Id, 
                    CaseNumber,
                    AlreadyCompletedViaSelfTrace)
  } 
  
  short_cases <- function(x) {
    dplyr::tbl(con, x) %>% 
      dplyr::filter(CreatedOn >= '20210104') %>%  
      dplyr::select(ContactId,
                    CaseNumber,
                    DateOfOnset,
                    DateOfSample,
                    Gender,
                    AgeAtPositiveResult,
                    CreatedOn) 
  }
  
  query <- short_locations(table1) %>%
    dplyr::left_join(short_collectcontactcalls(table2),
                     by = c("CollectCallId" = "Id"),
                     suffix = c("Locations", "CollectCloseContacts")) %>%
    dplyr::left_join(short_cases(table3), 
                     by = "CaseNumber", 
                     suffix = c("Merged", "Cases")) %>% 
    dplyr::filter(#!is.na(CaseNumber),
      #CaseFileStatus != 'Cancelled'
    ) 
  
  dplyr::show_query(query)
  data <- as.data.frame(query)
  message(paste0("Successfully retrieved data from ", table1, " & ", table2, " & ", table3, ". Filtered from 20210104 and Travelled Outside NI"))
  return(data)
}

# Get All Cases Information
getTableFilteredCases <- function(table1) {
  
  short_cases <- function(x) {
    dplyr::tbl(con, x) %>%
      dplyr::filter(DateOfSample >= '20210101') %>% 
      dplyr::select(CreatedOn,
                    CaseNumber,
                    CaseFileStatus)
  }
  
  query <- short_cases(table1) %>% 
    dplyr::filter(!is.na(CaseNumber),
                  !is.na(CreatedOn)) 
  
  dplyr::show_query(query)
  data <- as.data.frame(query)
  message(paste0("Successfully retrieved data from ", table1, ". Filtered from 20210104"))
  return(data)
}

# Get Synapse Tables
combinedtables <- getTableFilteredCombined("Locations", "CollectContactsCalls", "Cases") 

wgscases <- getTable("Wgscases")

cases <- getTableFilteredCases("Cases")

# Assign this weeks data
epiweek.number <- as.numeric(strftime(Sys.Date(), format = "%V"))

current.epiweek <- paste0("Epiweek", epiweek.number)
previous.epiweek <- paste0("Epiweek", epiweek.number-1)
two.epiweeks.ago <- paste0("Epiweek", epiweek.number-2)

# Define all cases for comparison
Allcases <- cases %>% 
  dplyr::filter(CreatedOn >= "2021-01-04" & CreatedOn <= Sys.Date()+1) %>% 
  dplyr::mutate(EpiweekCreated = paste0("Epiweek", strftime(CreatedOn, format = "%V")))


# Define travellers
## Make data frame

travellers <- combinedtables %>% 
  dplyr::filter(WhenDidYouReturnToNorthernIreland >= "2021-01-04" & WhenDidYouReturnToNorthernIreland <= Sys.Date()+1) %>% 
  dplyr::mutate(DateOfSample = as.Date(DateOfSample),
                Gender = as.character(Gender)) %>%
  dplyr::mutate(EpiweekReturned = paste0("Epiweek", strftime(WhenDidYouReturnToNorthernIreland, format = "%V"))) %>% 
  dplyr::mutate(EpiweekCreated = paste0("Epiweek", strftime(CreatedOn, format = "%V"))) %>% 
  dplyr::left_join(dplyr::select(wgscases,
                                 ContactId,
                                 WgsVariant,
                                 WgsReflexAssay), by = "ContactId")
                   
## Tidying
travellers$EpiweekCreated <-  sub('Epiweek0', 'Epiweek', travellers$EpiweekCreated) #Remove the 0 value from single digit Epiweeks
travellers$EpiweekReturned <-  sub('Epiweek0', 'Epiweek', travellers$EpiweekReturned)

travellers$WhenDidYouLeaveNorthernIreland <- strptime(travellers$WhenDidYouLeaveNorthernIreland, format = "%Y-%m-%d")
travellers$WhenDidYouReturnToNorthernIreland <- strptime(travellers$WhenDidYouReturnToNorthernIreland, format = "%Y-%m-%d") + lubridate::days(1) #Day behind fix
travellers$DateOfOnset <- strptime(travellers$DateOfOnset, format = "%Y-%m-%d")

travellers$CreatedOn <- as.Date(travellers$CreatedOn, format = "%Y-%m-%d")

travellers$WhenDidYouLeaveNorthernIreland <- format(travellers$WhenDidYouLeaveNorthernIreland, format = "%d-%m-%Y")
travellers$WhenDidYouReturnToNorthernIreland <- format(travellers$WhenDidYouReturnToNorthernIreland, format = "%d-%m-%Y")
travellers$DateOfOnset <- format(travellers$DateOfOnset, format = "%d-%m-%Y")

## Tidy pre drop down data
travellers$CountriesVisited <- lapply(travellers$CountriesVisited, stringr::str_trim)

source("./country_name_cleaner.R")

# Assign Common Travel Area
CTA <- as.vector(c("England", "Scotland", "Wales", "Isle of Man", "Guernsey", "Jersey", "ROI")) 

## Country tallies
country.count <- as.data.frame(table(travellers$CountriesVisited))
country.count <- dplyr::arrange(country.count, desc(Freq))

topten <- head(country.count, 10)
colnames(topten) <- c("country", "count")
colnames(topten) <- c("Country", "Count"
                      #, "Status"
)

colnames(travellers)[colnames(travellers) == "CountriesVisited"] <- "CountriesVisited"

####### REPORT DOWNLOADS  #######
system.date <- as.Date(Sys.Date(), format = "%d-%m-%Y")

# Previous Day
previous.day.report <- travellers %>%
  dplyr::filter(CreatedOn == Sys.Date()-1) %>% 
  dplyr::add_count(CountriesVisited, name = "TotalCases") %>%
  dplyr::select(CountriesVisited,
                TotalCases,
                AlreadyCompletedViaSelfTrace,
                WhenDidYouReturnToNorthernIreland,
                DateOfOnset,
                DateOfSample,
                CaseNumber,
                Gender,
                AgeAtPositiveResult,
                CreatedOn,
                AdditionalTravelInformation,
                MoreDetail,
                EpiweekCreated,
                EpiweekReturned
                #, Status
  ) %>%
  dplyr::select(-c(EpiweekCreated,
                   EpiweekReturned)) %>%
  dplyr::arrange(CountriesVisited)

# Current Week
current.week.report <- travellers %>% 
  dplyr::filter(EpiweekCreated == current.epiweek) %>% 
  dplyr::add_count(CountriesVisited, name = "TotalCases") %>%
  dplyr::select(CountriesVisited,
                TotalCases,
                AlreadyCompletedViaSelfTrace,
                WhenDidYouReturnToNorthernIreland,
                DateOfOnset,
                DateOfSample,
                CaseNumber,
                Gender,
                AgeAtPositiveResult,
                CreatedOn,
                AdditionalTravelInformation,
                MoreDetail,
                EpiweekCreated,
                EpiweekReturned
                #, Status
  ) %>%
  dplyr::select(-c(EpiweekCreated,
                   EpiweekReturned))%>%
  dplyr::arrange(CountriesVisited)

# Previous Week
previous.week.report <- travellers %>%
  dplyr::filter(EpiweekCreated == previous.epiweek) %>% 
  dplyr::add_count(CountriesVisited, name = "TotalCases") %>%
  dplyr::select(CountriesVisited,
                TotalCases,
                AlreadyCompletedViaSelfTrace,
                WhenDidYouReturnToNorthernIreland,
                DateOfOnset,
                DateOfSample,
                CaseNumber,
                Gender,
                AgeAtPositiveResult,
                CreatedOn,
                AdditionalTravelInformation,
                MoreDetail,
                EpiweekCreated,
                EpiweekReturned
                #, Status
  ) %>%
  dplyr::select(-c(EpiweekCreated,
                   EpiweekReturned))%>%
  dplyr::arrange(CountriesVisited)

#Previous 2 epiweeks
last2.epiweeks.report <- travellers %>%
  dplyr::filter((EpiweekCreated == previous.epiweek)|(EpiweekCreated == two.epiweeks.ago)) %>% 
  dplyr::add_count(CountriesVisited, name = "TotalCases") %>%
  dplyr::select(CountriesVisited,
                TotalCases,
                AlreadyCompletedViaSelfTrace,
                WhenDidYouReturnToNorthernIreland,
                DateOfOnset,
                DateOfSample,
                CaseNumber,
                Gender,
                AgeAtPositiveResult,
                CreatedOn,
                AdditionalTravelInformation,
                MoreDetail,
                EpiweekCreated,
                EpiweekReturned
                #, Status
  ) %>%
  dplyr::select(-c(EpiweekCreated, EpiweekReturned)) %>%
  dplyr::arrange(CountriesVisited)

#Cumlative report
culmulative.report <- travellers %>%
  dplyr::add_count(CountriesVisited, name = "TotalCases") %>%
  dplyr::select(CountriesVisited,
                TotalCases,
                AlreadyCompletedViaSelfTrace,
                WhenDidYouReturnToNorthernIreland,
                DateOfOnset,
                DateOfSample,
                CaseNumber,
                Gender,
                AgeAtPositiveResult,
                CreatedOn,
                AdditionalTravelInformation,
                MoreDetail,
                EpiweekCreated,
                EpiweekReturned
                #, Status
  ) %>%
  dplyr::select(-c(EpiweekCreated, EpiweekReturned)) %>%
  dplyr::arrange(CountriesVisited)


#to find outlying country combinations
culmulativereport_nested <- culmulative.report %>%
  dplyr::group_by(CountriesVisited) %>%
  tidyr::nest()

#####NOT COMPLETED YET
##Trying to split people who've traveled more than one place - 
#take first word of cpountriesvisited and create df with that if matches countries list, then do the same for second word and so on
culmulative.reportsorted <- culmulative.report %>%
  dplyr::mutate(CountryVector = as.character(CountriesVisited))

##REMOVE SPACES FROM ALL SPACED COUNTRIES
culmulative.reportsorted$CountryVector <- gsub("Antigua and Barbuda","AntiguaandBarbuda", culmulative.reportsorted$CountryVector)
culmulative.reportsorted$CountryVector <- gsub("Bosnia and Herzegovina","BosniaandHerzegovina", culmulative.reportsorted$CountryVector)
culmulative.reportsorted$CountryVector <- gsub("British Antarctic Territory","BritishAntarcticTerritory", culmulative.reportsorted$CountryVector)
culmulative.reportsorted$CountryVector <- gsub("British Indian Ocean Territory","BritishIndianOceanTerritory", culmulative.reportsorted$CountryVector)
culmulative.reportsorted$CountryVector <- gsub("Burkina Faso","BurkinaFaso ", culmulative.reportsorted$CountryVector)
culmulative.reportsorted$CountryVector <- gsub("Bosnia and Herzegovina ","BosniaandHerzegovina ", culmulative.reportsorted$CountryVector)

#SPLIT AT SPACE AND ALPHABETISE STRING
culmulative.reportsorted$CountryVector <- sapply(lapply(strsplit(culmulative.reportsorted$CountryVector, split = " \\s*"), sort), paste, collapse = " ")

culmulative.reportsorted$CountryVector <- gsub("[^[:alnum:][:blank:]]","", culmulative.reportsorted$CountryVector)

# End Script
DBI::dbDisconnect(con)

message("Data preparation script successfuly executed")

