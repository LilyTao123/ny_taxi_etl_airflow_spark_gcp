
import yaml
import argparse
import requests

# with open("config/project_config.yaml", 'r') as stream:
#     config = yaml.safe_load(stream)

def main(params):
    prefix = 'https://d37ci6vzurychx.cloudfront.net'
    service_type = params.service_type
    year = params.year
    month = int(params.month)
    
    # https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet
    
    url = f'{prefix}/trip-data/{service_type}_tripdata_{year}-{month:02d}.parquet'
    output = f'data/{service_type}/{service_type}_tripdata_{year}-{month:02d}.parquet'

    response = requests.get(url)
    if response.status_code == 200:
        with open(output, 'wb') as f:
            f.write(response.content)
            print('Data has downloaded')
    else: print('Data downloading failed')

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')

    parser.add_argument('--service_type', required=True, help='yellow or green')
    parser.add_argument('--year', required=True, help='the year of data')
    parser.add_argument('--month', required=True, help='the month of data')

    args = parser.parse_args()

    main(args)





