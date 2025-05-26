# Uniswap v3 Historical Data Exporter

This project fetches historical event data (swaps, mints, burns) for a specified Uniswap v3 pool using The Graph protocol and saves it to CSV files. This data can be used for backtesting trading strategies, liquidity provision analysis, or other research purposes.

## Features

- Fetches historical swaps, mints, and burns for a given Uniswap v3 pool.
- Uses The Graph for efficient querying of historical blockchain data.
- Implements robust pagination to retrieve comprehensive datasets for swaps, mints, and burns.
- Handles transient network errors with retries and delays.
- Writes data to CSV files incrementally, reducing memory usage for large datasets.
- Configurable via environment variables for pool address, RPC URL, and time range.

## Prerequisites

- Node.js (v16 or later recommended)
- npm (comes with Node.js)

## Setup & Configuration

1.  **Clone the repository:**
    ```bash
    git clone <repository_url> # Replace with the actual URL
    cd <repository_directory>  # Replace with the actual directory name
    ```

2.  **Install dependencies:**
    ```bash
    npm install
    ```

3.  **Configure Environment Variables:**
    The script is configured using environment variables. Create a `.env` file in the project root by copying the `.env.example` file:
    ```bash
    cp .env.example .env
    ```
    Then, edit the `.env` file with your specific parameters:

    *   **`INFURA_URL`**: Your Ethereum node provider RPC URL. This is essential for some SDK functionalities, though the primary data fetching relies on The Graph.
        *   *Example:* `INFURA_URL=https://mainnet.infura.io/v3/YOUR_INFURA_PROJECT_ID`

    *   **`POOL_ADDRESS`**: The contract address of the Uniswap v3 pool you want to query.
        *   *Example (USDC/WETH 0.05%):* `POOL_ADDRESS=0x88e6A0c2dDD26FEEb64F039a2c41296FcB3f5640`

    *   **`START_TIMESTAMP`** (Mandatory): The Unix timestamp (in seconds) for the beginning of the data fetching period.
        *   *Example (January 1, 2023, 00:00:00 UTC):* `START_TIMESTAMP=1672531200`

    *   **`END_TIMESTAMP`** (Mandatory): The Unix timestamp (in seconds) for the end of the data fetching period.
        *   *Example (January 2, 2023, 00:00:00 UTC):* `END_TIMESTAMP=1672617600`

    **Note:** `START_TIMESTAMP` and `END_TIMESTAMP` must be provided, and `START_TIMESTAMP` must be earlier than `END_TIMESTAMP`.

## Running the Script

Once you have configured your `.env` file, execute the following command in the project root:

```bash
npm start
```

This will run the `src/fetch_data.ts` script using `ts-node`. The script will fetch the data in batches and write it incrementally to the CSV files.

## Output

The script will generate the following CSV files in the project root:

-   `swaps.csv`: Contains swap event data.
-   `mints.csv`: Contains mint (liquidity provision) event data.
-   `burns.csv`: Contains burn (liquidity removal) event data.

### CSV File Headers:

**`swaps.csv`:**
`ID,Timestamp,Token0,Token1,Sender,Recipient,Amount0,Amount1,SqrtPriceX96,Tick`

**`mints.csv`:**
`ID,Timestamp,Owner,Sender,Origin,Amount,Amount0,Amount1,TickLower,TickUpper`

**`burns.csv`:**
`ID,Timestamp,Owner,Origin,Amount,Amount0,Amount1,TickLower,TickUpper`

## Important Notes

### Handling Large Datasets:
-   **Time:** Fetching data over extended periods (e.g., multiple months or years) can be very time-consuming. The script fetches data in batches with small delays to be polite to The Graph API.
-   **Connection & Power:** For very long fetches, ensure you have a stable internet connection and uninterrupted power supply to the machine running the script.
-   **File Size:** The output CSV files can become very large, potentially several gigabytes, depending on the pool's activity and the time range specified. Ensure you have adequate disk space.
-   **Rate Limits:** While the script has retries and delays, extremely long queries might still encounter API rate limits from The Graph. Consider breaking down very large time ranges into smaller, sequential runs if you face persistent issues.

## Future Improvements

-   Advanced data processing and feature engineering options.
-   Support for more types of historical data (e.g., liquidity snapshots at specific intervals).
-   Command-line arguments as an alternative or supplement to `.env` for configuration.
-   Option to choose output formats other than CSV (e.g., Parquet, JSON lines).
