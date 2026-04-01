library(tidyverse)
# Rank care events by frequency for each resident
read_csv("data/raw/care_events.csv") |>
  group_by(resident_id) |>
  summarize(total_care_events = n()) |>
  mutate(rank = dense_rank(desc(total_care_events))) |>
  arrange(rank) |>
  head(n = 20)