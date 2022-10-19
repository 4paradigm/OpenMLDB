# # This is needed to allow Airflow to pick up specific metadata fields it needs for certain features. We recognize
# it's a bit unclean to define these in multiple places, but at this point it's the only workaround if you'd like
# your custom conn type to show up in the Airflow UI.
def get_provider_info():
    return {
        "package-name": "airflow-provider-openmldb",  # Required
        "name": "OpenMLDB Airflow Provider",  # Required
        "description": "an airflow provider to connect OpenMLDB",  # Required
        "hook-class-names": ["openmldb_provider.hooks.openmldb_hook.OpenMLDBHook"],  # for airflow<2.2
        # "connection-types"
        "extra-links": ["openmldb_provider.operators.openmldb_operator.ExtraLink"], # unused
        "versions": ["0.0.1"]  # Required
    }
