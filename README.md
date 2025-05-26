# Uniswap v3 Historical Data Exporter

This project fetches historical event data (swaps, mints, burns) for a specified Uniswap v3 pool using The Graph protocol and saves it to CSV files. This data can be used for backtesting trading strategies, liquidity provision analysis, or other research purposes.

## Features

- Fetches historical swaps, mints, and burns for a given Uniswap v3 pool.
- Uses The Graph for efficient querying of historical blockchain data.
- Implements pagination to retrieve comprehensive datasets.
- Processes data into a user-friendly format.
- Saves data to separate CSV files for swaps, mints, and burns.
- Configurable for different pools and time ranges (though currently hardcoded, future improvement will be to use environment variables).

## Prerequisites

- Node.js (v16 or later recommended)
- npm (comes with Node.js)

## Setup & Installation

1.  **Clone the repository:**
    ```bash
    git clone <repository_url> # User will fill this in
    cd <repository_directory>  # User will fill this in
    ```
2.  **Install dependencies:**
    ```bash
    npm install
    ```
3.  **Configure the script (`src/fetch_data.ts`):**
    *   **RPC URL:** Update the `INFURA_URL` variable with your Ethereum node provider URL.
        *Example:* `const INFURA_URL = 'https://mainnet.infura.io/v3/YOUR_INFURA_PROJECT_ID';`
    *   **Pool Address:** Modify the `POOL_ADDRESS` variable to the Uniswap v3 pool you want to query.
        *Example (USDC/WETH 0.05%):* `const POOL_ADDRESS = '0x88e6A0c2dDD26FEEb64F039a2c41296FcB3f5640';`
    *   **Time Range:** Adjust `startTimestamp` and `endTimestamp` in the `main` function to define the period for data fetching. Timestamps are in seconds.
        *Example (for a specific hour):*
        ```typescript
        const endTimestamp = Math.floor(Date.now() / 1000); // Current time
        const startTimestamp = endTimestamp - (60 * 60); // 1 hour ago
        ```

## Running the Script

Execute the following command in the project root:

```bash
npm start
```

This will run the `src/fetch_data.ts` script using `ts-node`.

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

## Future Improvements

-   Use environment variables for configuration (RPC URL, Pool Address).
-   More robust error handling and retry mechanisms.
-   Advanced data processing and feature engineering options.
-   Support for more types of historical data (e.g., liquidity snapshots).
-   Command-line arguments for script parameters.
