#' Small script for combining election outcome files.
#' 
#' This script assumes it is located in: 
#'   <project folder>/script
#' 
#' With accompanying data in:
#'   <project folder>/data
#' 
#' It uses the `here` package to correctly set the working directory and find data files,
#' provided these assumptions are met.
#' 
#' It recursively searches the data folder for any files ending in `turnout.csv`,
#' loads each into memory, combines them into a single tibble, and parses the election
#' name into a province name. 
#' 
#' Specifically, this script is written for the Dutch upper house elections of 2023
#' (Provinciale Staten <province> 2023), and extracts the province name expecting this
#' pattern. Adaptation to other elections should be straightforward.

here::i_am("script/combine_files.R")
library(here)
library(tidyverse)

turnout_files <- dir(here("data/"), pattern = "turnout.csv$", full.names = TRUE, recursive = TRUE)

data <- turnout_files %>% 
    purrr::map(vroom::vroom, show_col_types = FALSE, progress = FALSE, .progress = TRUE) %>%
    purrr::list_rbind() %>%
    mutate(
        provincie = stringr::str_extract(contest_name, "Provinciale Staten (.*) 2023", group = 1)
    )

readr::write_csv(data, file = here("data/turnout.csv"))
