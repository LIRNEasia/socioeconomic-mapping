library(tidyverse)
library(caret)

# Read in the required data
census_data <- read.csv("data/census_features.csv")
mobile_data <- read.csv("data/mobile_features.csv")
satellite_data <- read.csv("data/satellite_features.csv")

# Clean the census data
## Remove unwanted variables
census_data <- census_data %>%
  select(
    -starts_with("age"),
    -starts_with("gen"),
    -starts_with("hou"),
    -starts_with("was")
  ) %>%
  mutate(
    coo_total = coo_total - coo_other,
    flo_total = flo_total - flo_other,
    lig_total = lig_total - lig_other,
    roo_total = roo_total - roo_other,
    ten_total = ten_total - ten_other,
    wal_total = wal_total - wal_other,
    wat_total = wat_total - wat_other
  ) %>%
  select(
    -coo_other,
    -flo_other,
    -lig_other,
    -roo_other,
    -ten_other,
    -wal_other,
    -wat_other
  )

## Convert counts to proportions
for (category in unique(substr(names(census_data), 1, 3))[-1]) {
  census_data <- census_data %>%
    mutate(
      across(
        .cols = starts_with(category),
        .fns = ~./.data[[paste0(category, "_total")]]
      )
    )
}
rm(category)

## Remove the total columns
census_data <- census_data %>% select(-ends_with("_total"))

# Conduct PCA
## Census data
pca_census <- preProcess(
  x = census_data %>% select(-gnd_id),
  method = "pca",
  pcaComp = 1
)

predict(pca_census, census_data) %>% write_csv("results/pca_census.csv")

## Mobile and satellite data
pca_mobile_satellite <- preProcess(
  x = left_join(mobile_data, satellite_data, by = "gnd_id") %>% select(-gnd_id),
  method = "pca",
  pcaComp = 1
)

predict(
  pca_mobile_satellite, 
  left_join(mobile_data, satellite_data, by = "gnd_id")
) %>% 
  write_csv("results/pca_mobile_satellite.csv")
