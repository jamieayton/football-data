


# 1. Setup ----------------------------------------------------------------

# global options
options(stringsAsFactors = FALSE)

# packages
library('tidyverse')
library('stringr')
library('rvest')

# dir & create /data/ subdir if doesn't exist
project_wd <- getwd()
data_dir <- paste0(project_wd, "/raw_data")
if(dir.exists(data_dir)==FALSE){dir.create(data_dir)}


# 2. Create list of csv files ---------------------------------------------

# page urls for different countries
page_urls <- tibble(
  country = c("england", "scotland", "germany", "italy", "spain", "france", "netherlands", "belgium", "portugal", "turkey", "greece")
  ) %>% 
  mutate(
    url = paste0("http://www.football-data.co.uk/", country, "m.php")
  )


# tbl of csv urls - to be populated via loop
csv_urls <- tibble(
  country = character(), last_update = character(), url = character()
)

# loop through page_urls and populate csv_urls
for (i in seq(1, nrow(page_urls))){
  
  # read page and wait to make sure not hitting server too regularly
  webpage <- read_html(page_urls$url[i])
  Sys.sleep(2.5 + runif(1))
  
  # parse data from the webpage
  csv_urls_i <- tibble(
    country = webpage %>% html_nodes("p:nth-child(1) > b") %>% html_text() %>% gsub("Data Files: ", "", .), 
    last_update = webpage %>% html_nodes("p:nth-child(1) > i") %>% html_text() %>% gsub("Last updated: \t", "", .) %>% as.character(.),
    url = webpage %>% html_nodes(css="a") %>% html_attr('href') %>% subset(., grepl(pattern=".csv", .)) %>% paste0("http://www.football-data.co.uk/", .)
  )
    
  # add rows for i to existing csv_urls tibble
  csv_urls <- csv_urls %>% 
    bind_rows(., csv_urls_i)


  rm(csv_urls_i)
  rm(webpage)
  
}

rm(i)


# parse url to get season_code & league_code
csv_urls <- csv_urls %>% 
  mutate(
    season_code = str_split(url, "/", simplify=TRUE)[,5], 
    season_code = paste0(
      str_sub(season_code, 1, str_length(season_code)/2), "-", str_sub(season_code, 1+str_length(season_code)/2, str_length(season_code))
      ), 
    league_code = str_split(url, "/", simplify=TRUE)[,6], 
    league_code = str_replace_all(league_code, "\\.csv", "")
  )

# create a filename for the csv file
csv_urls <- csv_urls %>% 
  mutate(
    file_code = paste0(league_code, "_", season_code), 
    file_name = paste0(data_dir, "/", file_code, ".csv")
  )





# 3. Get csv files --------------------------------------------------------

get_csv_files <- function(url, file_name){
  
  # read the csv from url
  csv_temp <- read.csv(url)
  
  # convert to tibble
  csv_temp <- as.tibble(csv_temp)
  
  # wait to prevent hitting server too regularly
  Sys.sleep(2.5 + runif(1))
  
  # write csv
  write_csv(csv_temp, file_name)
  
  rm(csv_temp)
}

# download csv files via loop
for (i in seq(1, nrow(csv_urls))){
  
  # get csv file
  get_csv_files(csv_urls$url[i], csv_urls$file_name[i])
  print(i)
}
rm(i)


# write csv_urls
write_csv(csv_urls, file.path(paste0("csv_urls", ".csv")))



# 4. Create parsed df with universal format -------------------------------

# read data
football_data <- map(csv_urls$file_name, read_csv) %>% 
  setNames(., csv_urls$file_code)


# desired columns to keep
desired_columns <- c(
  "Div", "Date", "HomeTeam", "AwayTeam", "FTHG", "FTAG", "FTR", "HS", "AS", "HST", "AST", 
  "B365H", "B365D", "B365A", "PSH", "PSD", "PSA", "PSCH", "PSCD", "PSCA", 
  "BbMxH", "BbMxD", "BbMxA", "BbAvH", "BbAvD", "BbAvA", "BbMx.2.5", "BbMx.2.5.1", "BbAv.2.5", "BbAv.2.5.1", 
  "BbAHh", "BbMxAHH", "BbMxAHA", "BbAvAHH" , "BbAvAHA"
)

# drop NAs
# select desired columns
# create id link back to original csv
# force formatting before binding
# drop NAs for critical cols
football_data <- map(
    seq_along(football_data), 
    function(x) football_data[[x]] %>% 
      select(one_of(desired_columns[desired_columns %in% colnames(.)])) %>% 
      mutate(
        id = names(football_data)[x]
      ) %>% 
      mutate(
        BbAHh = as.character(if(exists('BbAHh', where=.)){BbAHh} else {NA}), 
        PSCH = as.numeric(if(exists('PSCH', where=.)){PSCH} else {NA})
      )
  ) %>% 
  bind_rows(.) %>% 
  bind_cols(., str_split(.$id, "_", simplify=TRUE) %>% as.tibble()) %>% 
  rename(league_code = V1, season_code = V2) %>% 
  drop_na(one_of("Div", "Date", "HomeTeam", "AwayTeam", "FTHG", "FTAG", "FTR"))

rm(desired_columns)


# write
write_csv(football_data, paste0("football_data", ".csv"))




# 5. Cleanup --------------------------------------------------------------

rm(football_data, csv_urls, page_urls)
rm(project_wd, data_dir)
rm(get_csv_files)



