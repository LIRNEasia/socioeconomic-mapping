library(tidyverse)
library(caret)
library(spdep)
library(spatialreg)

source("pre-process.R")

# Create the neighbour weights
neighbour_train <- nb2listw(
  neighbours = poly2nb(
    pl = data_shapefile %>% filter(code_7 %in% rownames(data_train))
  ), 
  zero.policy = TRUE
)
neighbour_all <- nb2listw(
  neighbours = poly2nb(pl = data_shapefile), 
  zero.policy = TRUE
)

# Fit spatial regression models
# Assuming local effects (instead of global) so trying SDEM, SEM, SLX, and OLS
## Spatial Durbin Error Model (SDEM)
model_sdem <- errorsarlm(
  formula = pc_1 ~ .,  
  data =  data_train, 
  listw = neighbour_train, 
  Durbin = TRUE,
  zero.policy = TRUE
)

## Spatial Error Model (SEM)
model_sem <- errorsarlm(
  formula = pc_1 ~ .,  
  data =  data_train, 
  listw = neighbour_train, 
  Durbin = FALSE,
  zero.policy = TRUE
)

## Spatially Lagged X Model (SLX)
model_slx <- lmSLX(
  formula = pc_1 ~ ., 
  data =  data_train, 
  listw = neighbour_train, 
  zero.policy = TRUE
)

## Ordinary Least Squares (OLS)
model_ols <- lm(
  formula = pc_1 ~ ., 
  data =  data_train
)

# Test for spatial dependence
## Lagrange Multiplier test for presence of spatial dependence (bottom-up)
## p-value < 0.05 means upgrade
lm.LMtests(
  model = model_ols, 
  listw = neighbour_train, 
  zero.policy = TRUE, 
  test = "all"
)

## Likelihood ratio test for absense of spatial dependence (top-down)
## p-value < 0.05 means do not downgrade
LR.Sarlm(x = model_sdem, y = model_sem)
LR.Sarlm(x = model_sdem, y = model_slx)
LR.Sarlm(x = model_sdem, y = model_ols)

# Test for heteroscedasticity
bptest.Sarlm(object = model_sdem)

# Show the results of the selected model
summary(object = model_sdem, Nagelkerke = TRUE)

# Predict on test data
prediction_train <- predict(object = model_sdem)
prediction_test <- predict(
  object = model_sdem,
  newdata = data_test %>% na.omit(),
  listw = neighbour_all,
  zero.policy = TRUE
)

# Write the results
list("gnd_id" = as.character(data_shapefile$code_7)) %>%
  as.data.frame() %>%
  left_join(
    y = rbind(
      as.data.frame(prediction_train), 
      as.data.frame(prediction_test)
    ) %>%
      rownames_to_column("gnd_id"),
    by = "gnd_id"
  ) %>%
  write.csv(file = "results/sr_freq_sdem.csv", row.names = FALSE)
