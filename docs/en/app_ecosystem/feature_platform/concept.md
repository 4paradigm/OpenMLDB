## Introduction

The OpenMLDB Feature Platform is a sophisticated feature store service, leveraging [OpenMLDB](https://github.com/4paradigm/OpenMLDB) for efficient feature management and orchestration.



* Feature: Data obtained through feature extraction from raw data that can be directly used for model training and inference.
* Feature View: A set of features defined by a single SQL computation statement.
* Data Table: In OpenMLDB, data tables include online storage that supports real-time queries and distributed offline storage.
* Online Scenario: By deploying online feature services, it provides hard real-time online feature extraction interfaces using online data.
* Offline Scenario: Uses distributed computing to process offline data for feature computation and exports sample files needed for machine learning.
* Online-Offline Consistency: Ensuring that the feature results computed in online and offline scenarios are consistent through the same SQL definitions.
