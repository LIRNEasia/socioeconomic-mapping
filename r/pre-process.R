library(tidyverse)
library(caret)
library(spdep)

# Read in the required data
data_raw <- inner_join(
  read.csv(file = "data/mobile_features.csv"), 
  read.csv(file = "data/satellite_features.csv"),
  by = "gnd_id"
) %>%
  left_join(read.csv(file = "results/pca_census.csv"), by = "gnd_id") %>%
  rename(pc_1 = PC1) %>%
  column_to_rownames("gnd_id")

# Log transform skewed variables
data_unskewed <- data_raw %>%
  transmute(
    call_count,
    avg_call_duration,
    nighttime_call_count,
    avg_nighttime_call_duration,
    incoming_call_count,
    avg_incoming_call_duration,
    radius_of_gyration_log = log(radius_of_gyration),
    unique_tower_count,
    spatial_entropy,
    avg_call_count_per_contact,
    avg_call_duration_per_contact,
    contact_count,
    social_entropy,
    travel_time_major_cities_log = log(travel_time_major_cities + 1),
    population_count_worldpop_log = log(population_count_worldpop + 1),
    population_count_ciesin_log = log(population_count_ciesin + 1),
    population_density_log = log(population_density),
    aridity_index,
    evapotranspiration,
    nighttime_lights,
    elevation_log = log(elevation + 1),
    vegetation,
    distance_roadways_motorway,
    distance_roadways_trunk_log = log(distance_roadways_trunk + 1),
    distance_roadways_primary_log = log(distance_roadways_primary + 1),
    distance_roadways_secondary_log = log(distance_roadways_secondary + 1),
    distance_roadways_tertiary_log = log(distance_roadways_tertiary + 1),
    distance_waterways_log = log(distance_waterways + 1),
    urban_rural_fb_log = log(urban_rural_fb + 1),
    urban_rural_ciesin,
    global_human_settlement_log = log(global_human_settlement + 1),
    protected_areas_log = log(protected_areas + 1),
    land_cover_woodland,
    land_cover_grassland_log = log(land_cover_grassland + 1),
    land_cover_cropland,
    land_cover_wetland_log = log(land_cover_wetland + 1),
    land_cover_bareland_log = log(land_cover_bareland + 1),
    land_cover_urban,
    land_cover_water_log = log(land_cover_water + 1),
    pregnancies_log = log(pregnancies),
    births_log = log(births),
    precipitation,
    temperature,
    pc_1
  )

# Partition data into test and train splits controlling for province
set.seed(2022)
indices_train <- createDataPartition(
  y = as.factor(substr(rownames(data_unskewed), 1, 1)),
  p = 0.8,
  list = FALSE
) %>%
  as.vector()

# Normalize and remove highly correlated and near-zero variance variables
preprocess <- preProcess(
  x = data_unskewed[indices_train, ] %>% select(-pc_1),
  method = c("center", "scale", "corr", "nzv"),
  cutoff = 0.80
)

data_train <- predict(object = preprocess, data_unskewed[indices_train, ])
data_test <- predict(object = preprocess, data_unskewed[-indices_train, ])

# Join the data with the shapefile
data_shapefile <- st_read(dsn = "data/sri_lanka_gnd", layer = "sri_lanka_gnd")
rownames(data_shapefile) <- data_shapefile$code_7

rm(data_raw, data_unskewed, indices_train, preprocess)

