


# 01 - setup --------------------------------------------------------------

# packages
library('tidyverse')
library('here')

# dir & create /data/ subdir if doesn't exist
project_dir <- here::here()



# 02 - read data ----------------------------------------------------------

# read football_data csv
football_data <- read_csv("football_data.csv", col_types = cols(.default = "c"))



# 03 - filter data --------------------------------------------------------

# filter cols, leagues, seasons
football_data <- football_data %>% 
  select(
    league_code, season_code, Div, Date, HomeTeam, AwayTeam, FTR
  ) %>% 
  filter(
    league_code %in% c("E0", "E1", "E2", "E3", "SC0", "D1", "F1", "I1", "SP1")
  ) %>% 
  filter(
    season_code %in% c("1819")
  )



# 04 - write data ---------------------------------------------------------

write_csv(football_data, "true_odds_data.csv")



# 05 - clean up -----------------------------------------------------------

rm(football_data)
rm(project_dir)


