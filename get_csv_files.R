


# 1. Setup ----------------------------------------------------------------

# packages
library('tidyverse')
library('lubridate')
library('furrr')
library('rvest')
library('here')

# dir & create /data/ subdir if doesn't exist
project_dir <- here::here()
data_dir <- file.path(project_dir, "raw_data")
if(dir.exists(data_dir)==FALSE){dir.create(data_dir)}



# 2. Competitions List ----------------------------------------------------

# page urls for different countries
page_urls <- 
  tribble(
    ~country, 
    "england", 
    "scotland", 
    "germany", 
    "italy", 
    "spain", 
    "france", 
    "netherlands", 
    "belgium", 
    "portugal", 
    "turkey", 
    "greece"
  ) %>% 
  mutate(
    url = paste0("http://www.football-data.co.uk/", country, "m.php")
  )



# 3. Scrape Competitions --------------------------------------------------

# tbl of csv urls - to be populated via loop
csv_urls <- 
  tibble(
    country = character(), 
    last_update = character(), 
    url = character()
  )

# loop through page_urls and populate csv_urls
for (i in seq_along(page_urls$url)){
  
  # read page and wait to make sure not hitting server too regularly
  webpage <- read_html(page_urls$url[i])
  Sys.sleep(2.5 + 0.5*runif(1))
  
  # parse data from the webpage
  csv_urls_i <- 
    tibble(
      country = webpage %>% 
        html_nodes(css = "p:nth-child(1) > b") %>% 
        html_text() %>% 
        str_replace_all("Data Files: ", ""), 
      last_update = webpage %>% 
        html_nodes("p:nth-child(1) > i") %>% 
        html_text() %>% 
        str_replace_all("Last updated: \t", "") %>% 
        as.character(),
      url = webpage %>% 
        html_nodes(css = "a") %>% 
        html_attr('href') %>% 
        str_subset(pattern = ".csv$") %>% 
        paste0("http://www.football-data.co.uk/", .)
    )
  
  # add rows for i to existing csv_urls tibble
  csv_urls <- csv_urls %>% 
    bind_rows(., csv_urls_i)
  
  # rm
  rm(csv_urls_i)
  rm(webpage)
  
}

# check completion
stopifnot(
  identical(i, length(page_urls$url))
)
rm(i)


# parse url to get season_code & league_code
csv_urls <- csv_urls %>% 
  mutate(
    parsed_url = url, 
    parsed_url = parsed_url %>% str_replace_all("http://www.football-data.co.uk/", ""), 
    parsed_url = parsed_url %>% str_replace_all(".csv", "")
  ) %>% 
  separate(
    parsed_url, into = c("string", "season_code", "league_code"), sep = "\\/"
  ) %>% 
  select(
    country, last_update, url, league_code, season_code
  )

# create a filename for the csv file
csv_urls <- csv_urls %>% 
  mutate(
    file_code = paste0(league_code, "_", season_code), 
    file_name = file.path(data_dir, paste0(file_code, ".csv")) %>% as.character()
  )



# 4. Download Files -------------------------------------------------------

# loop through and download csv files
for (i in seq_along(csv_urls$url)){
  
  # download via read_csv
  csv_temp <- read.csv(csv_urls$url[i])
  
  # convert to tibble
  csv_temp <- as.tibble(csv_temp)
  
  # write csv
  write_csv(csv_temp, csv_urls$file_name[i])
  
  # rm
  rm(csv_temp)
  
  # wait to prevent hitting server too regularly
  Sys.sleep(2.5 + 1.5*runif(1))
  
  # progress
  print(paste0(i, " of ", nrow(csv_urls)))
}

# check completion
stopifnot(
  identical(i, nrow(csv_urls))
)
rm(i)



# 5. Create parsed df with universal format -------------------------------

# read data
football_data <- csv_urls %>% 
  select(league_code, season_code, file_name) %>% 
  mutate(
    data = future_map(file_name, function(x) read_csv(x, col_types = cols(.default = col_character())))
  ) %>% 
  select(-file_name) %>% 
  unnest()

# clean data & select columns
football_data <- football_data %>% 
  drop_na("Div") %>% 
  mutate(
    HomeTeam = if_else(is.na(HomeTeam) & !is.na(HT), HT, HomeTeam), 
    AwayTeam = if_else(is.na(AwayTeam) & !is.na(AT), AT, AwayTeam)
  ) %>% 
  select(
    league_code, season_code, 
    Div, Date, 
    ends_with("Team"), 
    starts_with("FT"), 
    ends_with("S"), 
    ends_with("ST"), 
    starts_with("B365"), 
    starts_with("BbAv"), 
    starts_with("BbMx"), 
    starts_with("BbAH"), 
    starts_with("PS")
  ) %>% 
  select(
    -starts_with("B365."), 
    -starts_with("B365A")
  )

# change col types
football_data <- football_data %>% 
  mutate(
    Date = dmy(Date)
  ) %>% 
  mutate_at(
    .vars = c("FTHG", "FTAG", "HS", "AS", "HST", "AST"), 
    .funs = function(x) x %>% as.integer()
  ) %>% 
  mutate_at(
    .vars = vars(starts_with("B365|PS")), 
    .funs = function(x) x %>% as.numeric()
  ) %>% 
  mutate_at(
    .vars = vars(starts_with("Bb"), -starts_with("BbAH")), 
    .funs = function(x) x %>% as.numeric()
  )

# fix incorrect data
football_data <- football_data %>% 
  mutate_at(
    .vars = vars(starts_with("B365|BbAv|BbMx|PS")), 
    .funs = function(x) if_else(near(x, 0), NA, x)
  ) %>% 
  mutate_at(
    .vars = vars(starts_with("B365|BbAv|BbMx|PS")), 
    .funs = function(x) if_else(x < 1.0, NA, x)
  )



# 6. Write Data -----------------------------------------------------------

# write csv files
write_csv(page_urls, file.path(paste0("page_urls", ".csv")))
write_csv(csv_urls, file.path(paste0("csv_urls", ".csv")))
write_csv(football_data, file.path(paste0("football_data", ".csv")))



# 7. Cleanup --------------------------------------------------------------

# data
rm(football_data, csv_urls, page_urls)

# dir
rm(project_dir, data_dir)


