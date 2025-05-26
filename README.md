# Uniswap v3 OHLC and Liquidity Data Exporter

This project fetches raw historical event data (swaps, mints, burns) for a specified Uniswap v3 pool using The Graph protocol. It then processes this data to generate a single CSV file containing time-bucketed Open, High, Low, Close (OHLC) prices, along with an analysis of active liquidity within each OHLC price range and its ratio to the total liquidity in the pool. This output is designed for financial analysis, backtesting trading strategies, and liquidity research.

## Features

- Fetches historical swaps, mints, and burns for a given Uniswap v3 pool using The Graph.
- Implements robust pagination and retry mechanisms for reliable data fetching.
- Configurable via environment variables for pool address, RPC URL, and the data fetching time range.
- Calculates Open, High, Low, Close (OHLC) data for configurable time intervals.
- Computes the total active liquidity within each OHLC candle's high-low price range.
- Calculates the ratio of this active liquidity to the total liquidity in the pool for each interval.
- Outputs a single, comprehensive CSV file (`historical_pool_data.csv`) for analysis.

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

    *   **Time Bucket Interval:** The interval for OHLC data is currently hardcoded as `BUCKET_INTERVAL_SECONDS` in `src/fetch_data.ts` (defaulted to 3600 seconds, i.e., 1 hour). To change the interval, you will need to modify this constant directly in the script.

## Running the Script

Once you have configured your `.env` file, execute the following command in the project root:

```bash
npm start
```

This will run the `src/fetch_data.ts` script using `ts-node`. The script will first fetch all raw event data into memory, then process it to generate the final output file.

## Output: `historical_pool_data.csv`

The script generates a single CSV file named `historical_pool_data.csv` in the project root.

### CSV File Headers:

*   **`Time`**: Unix timestamp (in seconds) for the start of the time bucket.
*   **`Open`**: Opening price for the bucket (price of token1 in terms of token0).
*   **`High`**: Highest price observed in the bucket.
*   **`Low`**: Lowest price observed in the bucket.
*   **`Close`**: Closing price for the bucket.
*   **`ActiveLiquidityInRange`**: Total liquidity (as a string, representing a potentially large number) that was within the tick range corresponding to the bucket's High and Low prices. This represents the sum of liquidity from all positions that were active across any part of this price range during the bucket's duration.
*   **`TotalLiquidityInPool`**: Total liquidity in the pool at the end of the bucket (as a string, representing a potentially large number).
*   **`LiquidityRatio_ActiveInRange_vs_TotalInPool`**: The ratio of `ActiveLiquidityInRange` to `TotalLiquidityInPool`, expressed as a decimal (e.g., 0.75 for 75%).

## Important Notes

### Handling Large Datasets:
-   **Time:** Fetching raw event data over extended periods (e.g., multiple months or years) can be very time-consuming due to the sheer volume of events.
-   **Connection & Power:** For very long fetches, ensure a stable internet connection and uninterrupted power.
-   **Memory Usage:** The script now loads all raw swap, mint, and burn data into memory for the specified time range before processing. For very long time ranges or extremely active pools, this could lead to high memory consumption. Consider processing data in smaller chronological chunks if memory issues arise.
-   **File Size:** The output `historical_pool_data.csv` file can still be large, depending on the time range and bucket interval. Ensure adequate disk space.
-   **Rate Limits:** While the script has retries and delays, extremely long queries for raw data might still encounter API rate limits from The Graph. Consider breaking down very large time ranges into smaller, sequential runs.

## Future Improvements

-   Further advanced data processing and feature engineering options (e.g., volume, volatility measures).
-   Support for more types of historical data or alternative liquidity metrics.
-   Making `BUCKET_INTERVAL_SECONDS` configurable via environment variable or command-line argument.
-   Command-line arguments as an alternative or supplement to `.env` for other configurations.
-   Option to choose output formats other than CSV (e.g., Parquet, JSON lines).
