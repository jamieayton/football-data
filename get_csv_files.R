


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
    file_name = paste0(data_dir, "/", league_code, "_", season_code, ".csv")
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




