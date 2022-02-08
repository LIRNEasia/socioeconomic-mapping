library(tidyverse)

# Read in the data
mobile_features_tower <- read.csv("data/mobile_features_tower.csv") %>%
  rename_with(.fn = ~tolower(.)) %>%
  rename(tower_id = home_tower_id)
intersection_areas <- read.csv("data/intersection_areas.csv")
gnd_areas <- read.csv("data/gnd_areas.csv")

# Map features from tower level to GND level
mobile_features_gnd <- intersection_areas %>%
  left_join(gnd_areas, by = "gnd_id") %>%
  left_join(mobile_features_tower, by = "tower_id") %>%
  mutate(proportion = intersection_area/gnd_area) %>%
  mutate(across(.cols = call_count:social_entropy, .fns = ~.*proportion)) %>%
  group_by(gnd_id) %>%
  summarise(across(call_count:social_entropy, .fns = ~sum(.))) %>%
  filter(gnd_id != 0)

# Write the results to disk
write.csv(mobile_features_gnd, "data/mobile_features.csv", row.names = FALSE)
