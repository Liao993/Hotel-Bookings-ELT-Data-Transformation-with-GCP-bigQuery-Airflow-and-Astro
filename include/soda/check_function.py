import logging
from soda.scan import Scan # type: ignore

# Configure logging for Airflow
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def check(scan_name, checks_subpath=None, data_source='bookings', project_root='include',
          config_file=None, test_connection=False):

    logger.info('Running Soda Scan for scan_name: %s', scan_name)
    
    # Default config file path if not provided
    config_file = config_file or f'{project_root}/soda/configuration.yml'
    checks_path = f'{project_root}/soda/checks'

    if checks_subpath:
        checks_path += f'/{checks_subpath}'

    scan = Scan()
    scan.set_verbose()
    scan.add_configuration_yaml_file(config_file)
    scan.set_data_source_name(data_source)
    # Only add checks if not testing connection
    if not test_connection:
        scan.add_sodacl_yaml_files(checks_path)
   
    scan.set_scan_definition_name(scan_name)

    # Execute the scan
    result = scan.execute()
    logger.info('Soda Scan logs:\n%s', scan.get_logs_text())

    # Extract and log detailed scan results
    if not test_connection:
        scan_results = scan.get_scan_results()
        check_results = scan_results.get('checkOutcomes', [])
        logger.info('Soda Scan Results for %s:', scan_name)
        for check in check_results:
            check_name = check.get('name', 'Unnamed Check')
            outcome = check.get('outcome', 'Unknown')
            diagnostics = check.get('diagnostics', {})
            logger.info('Check: %s, Outcome: %s, Diagnostics: %s', check_name, outcome, diagnostics)
    else:
        logger.info('Connection test completed successfully.')

    if result != 0:
        logger.error('Soda Scan failed with exit code: %s', result)
        raise ValueError(f'Soda Scan failed with exit code: {result}')

    return result