package com._4paradigm.openmldb.taskmanager.config;

/**
 * The exception for incorrect configuration.
 */
public class ConfigException extends Exception {

    public ConfigException(String config, String message) {
        super(String.format("Error of config '%s': %s", config, message));
    }

    public ConfigException(String message) {
        super(message);
    }

}
