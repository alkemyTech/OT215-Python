import logging

def log_dag ():
    '''
    logs configuration %Y-%m-%d-name_logger-message
    '''
    logger = logging.getLogger('dag_logger')
    logging.basicConfig(format='%(asctime)s %(logger)s %(message)s', datefmt='%Y-%m-%d', filename='log_dag.log', encoding='utf-8', level=logging.DEBUG)
    logging.info("Logs start...")