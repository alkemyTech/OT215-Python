
LOG_CFG = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'default_formatter': {
            'format': '%(asctime)s-%(name)s-%(message)s',
            'datefmt': '%Y-%m-%d'
        },
    },
    'handlers': {
        'stream_handler': {
            'class': 'logging.StreamHandler',
            'formatter': 'default_formatter',
            'stream': 'ext://sys.stdout',
            'level': 'INFO'
        },
    },
    'loggers': {
        'DAG': {
            'handlers': ['stream_handler'],
            'propagate': False,
        }
    }
}
