# Version Upgrade

## Introduction

FeatInsight provides an HTTP interface to the external users, relying on the OpenMLDB database for storing metadata. Therefore, version upgrades can be carried out using methods like multiple instances and rolling updates.

## Single Instance Upgrade Steps
1. Download the new installation package or Docker image.
2. Stop the currently running instance of FeatInsight.
3. Start a new instance with the new FeatInsight package.