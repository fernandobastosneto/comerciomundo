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

# baixar efetivamente os dados

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

  data.table::fread(paste0("unzip -p ", file)) %>%
    janitor::clean_names() %>%
    .[, .(year, aggregate_level, trade_flow_code, reporter_code, partner_code, commodity_code,
          trade_value_us)] %>%
    .[aggregate_level == 2] %>%
    data.table::fwrite(paste0(here::here("data-raw/"), filename, ".csv"))
}


purrr::walk(files, write_comtrade)

files_csv <- fs::dir_ls(here::here("data-raw"), regexp = ".csv$")

comtrade <- purrr::map_df(files_csv, vroom::vroom)

usethis::use_data(comtrade)

url <- "https://comtrade.un.org/data/cache/reporterAreas.json"

httr::GET(url, httr::write_disk(here::here("data-raw", "dic_reporters.json")))

dic_reporters <- jsonlite::fromJSON(here::here("data-raw", "dic_reporters.json"), simplifyDataFrame = T)

dic_reporters <- dic_reporters$results %>%
  tibble::as_tibble()

usethis::use_data(dic_reporters)

url <- "https://comtrade.un.org/data/cache/partnerAreas.json"

httr::GET(url, httr::write_disk(here::here("data-raw", "dic_partners.json")))

dic_partners <- jsonlite::fromJSON(here::here("data-raw", "dic_partners.json"), simplifyDataFrame = T)

dic_partners <- dic_partners$results %>%
  tibble::as_tibble()

usethis::use_data(dic_partners)

# Nomes dos países - do MDIC ao Comtrade

dic_comtrade <- comerciomundo::dic_reporters %>%
  dplyr::mutate(id = as.numeric(id)) %>%
  tidyr::drop_na()

dic_mdic <- readr::read_csv2(here::here("data-raw", "dic_paises_comtrade_mdic.csv")) %>%
  janitor::clean_names() %>%
  dplyr::select(co_pais_ison3, no_pais, no_pais_ing) %>%
  dplyr::rename(id = co_pais_ison3) %>%
  dplyr::mutate(id = as.numeric(id))

dic_comtrade_mdic <- dic_mdic %>%
  dplyr::left_join(dic_comtrade)

usethis::use_data(dic_comtrade_mdic)
