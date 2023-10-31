# Data Insertion with Worker Pool, Connection Pool, and Failover Mechanism

This Go program is designed to efficiently insert one million data records into a database using a combination of a worker pool, connection pooling, and a failover mechanism.

## Features

- Utilizes a worker pool to parallelize data insertion for improved performance.
- Implements a connection pool to efficiently manage and reuse database connections.
- Includes a failover mechanism for handling database connection errors and retries.
- Supports inserting data from a CSV file into a MySQL database.
- Utilizes the [github.com/go-sql-driver/mysql](https://github.com/go-sql-driver/mysql) driver for MySQL database connectivity.

## Prerequisites

Before using this program, ensure you have the following prerequisites installed:

- Go: You can download and install Go from [golang.org](https://golang.org/dl/).

## Data Source
The `majestic-million.csv` file is used as the source data for this project. This file is freely available under the [CCA3 License](https://creativecommons.org/licenses/by/3.0/#:~:text=Share%20%E2%80%94%20copy%20and%20redistribute%20the%20material%20in,as%20long%20as%20you%20follow%20the%20license%20terms.). It contains a list of the top one million websites.

You can download the file from the following link: [majestic_million.csv](http://downloads.majestic.com/majestic_million.csv).

Please make sure to review and comply with the terms of the CCA3 License when using this data.

## Configuration

Make sure to set up the required environment variables in a `.env` file in the root of your project. You can use the [github.com/joho/godotenv](https://github.com/joho/godotenv) library to load these variables. Here are the necessary environment variables:

- `DB_USERNAME`: Your database username.
- `DB_NAME`: The name of your database.

## Usage

1. Clone this repository to your local machine:

   ```bash
   git clone https://github.com/umjiiii/worker-pool-connection.git
   cd your-repo
2. Create a .env file in the project root directory and add the required environment variables as mentioned in the Configuration section.
3. Build and run the program:
   ```bash
   go build
   ./your-repo

4. The program will read data from the majestic_million.csv file (adjust the file name as needed) and insert it into the MySQL database using the specified worker pool size and connection pooling.
5. Monitor the program's progress as it inserts data into the database, and it will report the time taken once the insertion is complete.

## Customization
You can customize this program to fit your specific needs by adjusting the following parameters in the code:

- `dbMaxConns` and `dbMaxIdleConns`: Adjust these constants to configure the maximum open and maximum idle database connections.

- `totalWorker`: Set the number of worker goroutines to control parallelism during data insertion.

- `csvFile`: Change the CSV file name to match your data source.

## License and Attribution
This program is based on and inspired by the work of the original author [NovalAgung - Dasar Pemrograman Golang](https://github.com/novalagung/dasarpemrogramangolang-example/tree/master/chapter-D.1-insert-1mil-csv-record-into-db-in-a-minute), and it is distributed under the terms of the original license (Link to the Original License).

Feel free to use, modify, and distribute this program while respecting the terms and conditions of the original license. Ensure that you provide proper attribution to the original author and adhere to any license requirements as specified in the original repository.

If you find any issues or want to contribute to the development of this program, please create a pull request or open an issue on the repository.
