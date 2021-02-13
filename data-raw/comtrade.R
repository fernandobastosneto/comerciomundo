library(magrittr)

# Avaliar a disponibilidade dos dados
url_base_availability <- "http://comtrade.un.org/api/refs/da/bulk?"
r <- "all"
freq <- "A"
ps <- "2018"
px <- "HS"
type <- "C"
token <- "t/waZ8Eu4BGH1yRU3KHnyiv6BspmGbDDkTAjMbfJNXhY1aJf5XUHjhM/McT4XnrUT84ZstrnCPyZaKGNSUQdTpymWHXyflX+dU89pkjILinwN9a7goI4VdO51hWRApE4Gegz12xS4fPNTep16SBEs7IAI//8UIiqC14n6joag9UquzefV2/WO7PGhhPz/VPW"

url <- paste0(url_base, "r=", r, "&freq=", freq, "&ps=", ps, "&px=", px,
              "&type=", type, "&token=", token)

req <- httr::GET(url)

#

get_comtrade <- function(ano) {

  r <- "all"
  freq <- "A"
  px <- "HS"
  type <- "C"
  token <- Sys.getenv("COMTRADE_TOKEN")

  url_base <- "http://comtrade.un.org/api/get/bulk/"
  url <- paste0(url_base, type, "/", freq, "/", ano, "/", r, "/", px, "?token=", token)
  httr::GET(url, httr::write_disk(paste0(here::here("data-raw/"), ano, "_comtrade.zip"),
                                  overwrite = T))

}

# purrr::walk(2013:2018, get_comtrade)

files <- fs::dir_ls(here::here("data-raw"), regexp = ".zip$")

write_comtrade <- function(file) {

  filename <- file %>%
    stringr::str_extract("\\d{4}_comtrade(?=.zip$)")

  vroom::vroom(file,
               col_select = c(Year, `Aggregate Level`, `Trade Flow Code`, `Reporter Code`,
                              `Partner Code`, `Commodity Code`, `Trade Value (US$)`)) %>%
    janitor::clean_names() %>%
    dplyr::filter(aggregate_level == 2) %>%
    vroom::vroom_write(paste0(here::here("data-raw/"), filename, ".csv"))
}

purrr::walk(files, write_comtrade)

files_csv <- fs::dir_ls(here::here("data-raw"), regexp = ".csv$")

comtrade <- purrr::map_df(files_csv, vroom::vroom)

usethis::use_data(comtrade)
