# New York Rideshare Analysis Using PySpark
In these script files, through comprehensive analysis including data pre-processing, insights into trip counts, profits for each business, top K-processing, route analysis, graph processing etc, analysis of the New York 'Uber/Lyft' data from January 1, 2023, to May 31, 2023 using various Spark techniques.

## Datasets
- `rideshare_data.csv`: Contains detailed information about rideshare trips, including pickup and dropoff locations, trip length, request to pickup time, total ride time, and various financial metrics.
- `taxi_zone_lookup.csv`: Contains information about different taxi zones, including borough, zone, and service zone.
- `joined_df.csv`: The merged dataset resulting from Task 1, including additional borough and zone information for each trip.

the datasets are yet to be uploaded

## Project Structure 
- `scripts/`: spark files containing comprehensive analysis of the New York 'Uber/Lyft' data from January 1, 2023, to May 31, 2023 using various Spark techniques & also a python notebook containing visualisations. 
- `output files/`: csv files from the analysis performed

### File Descriptions 
- `scripts/`<br>
`dataanalysis.py`: Spark script for data analysis tasks, including merging datasets, aggregating data, and calculating averages.<br>
`graphanalysis.py`: Spark script for graph-related tasks, including top-K processing, route analysis, and Task 8 (graph processing).<br>
`visualisations.ipynb`: Jupyter notebook containing scripts for visualizing the results of data analysis and graph processing.<br>
- `output files/`<br>
`/task 2/driver_earnings.csv`: Data on driver's earnings per business per month. <br>
`/task 2/platform_profits.csv`: Data on platform's profits per business per month.<br>
`/task 2/trips_count.csv`: Data on trip counts per business per month.<br>
`/task 5/avg_waiting_time.csv`: Data on average waiting time for rideshare pickups in January.
