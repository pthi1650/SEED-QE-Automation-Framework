from custom_conf.initialization import initialize_config


def test_vault():

    conf_manager = initialize_config(
        team_key='seed_intl_pgm', environment='stg',
        remote_config_src_type='vault', allow_remote_update=True
    )

    print(conf_manager.settings.items())

