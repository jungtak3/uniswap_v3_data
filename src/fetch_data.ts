import { ethers } from 'ethers';
import { Pool, TickListDataProvider, Tick } from '@uniswap/v3-sdk';
import { GraphQLClient, gql } from 'graphql-request';
import { createObjectCsvWriter } from 'csv-writer';
import { CsvWriter } from 'csv-writer/src/lib/csv-writer'; // For typing
import * as dotenv from 'dotenv';

// Initialize dotenv to load environment variables
dotenv.config();

// Configuration constants/variables from environment
const INFURA_URL = process.env.INFURA_URL || 'YOUR_RPC_URL_HERE';
const POOL_ADDRESS = process.env.POOL_ADDRESS || '0x88e6A0c2dDD26FEEb64F039a2c41296FcB3f5640'; // Default Example: USDC/WETH 0.05% pool
const THE_GRAPH_URL = 'https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v3';
const MAX_RECORDS_PER_QUERY = 100; 
const MAX_RETRIES = 3; // Max retries for GraphQL requests
const RETRY_DELAY_MS = 5000; // Delay between retries in milliseconds
const BATCH_DELAY_MS = 200; // Delay between successful batch requests in milliseconds


// ABI for IUniswapV3Pool
const IUniswapV3PoolABI = [
  "function token0() external view returns (address)",
  "function token1() external view returns (address)",
  "function fee() external view returns (uint24)",
  "function liquidity() external view returns (uint128)",
  "function slot0() external view returns (uint160 sqrtPriceX96, int24 tick, uint16 observationIndex, uint16 observationCardinality, uint16 observationCardinalityNext, uint8 feeProtocol, bool unlocked)"
];

interface SwapData {
  id: string;
  timestamp: string;
  token0: string;
  token1: string;
  sender: string;
  recipient: string;
  amount0: string;
  amount1: string;
  sqrtPriceX96: string;
  tick: string;
}

interface MintData {
  id: string;
  timestamp: string;
  owner: string;
  sender: string;
  origin: string;
  amount: string; // Liquidity amount, typically integer
  amount0: string; // Token amount, decimal string
  amount1: string; // Token amount, decimal string
  tickLower: string;
  tickUpper: string;
}

interface BurnData {
  id: string;
  timestamp: string;
  owner: string;
  origin: string;
  amount: string; // Liquidity amount, typically integer
  amount0: string; // Token amount, decimal string
  amount1: string; // Token amount, decimal string
  tickLower: string;
  tickUpper: string;
}

// Define a type for our CSV writer instances for better type safety
type ObjectCsvWriter = CsvWriter<object>;


// Helper function to introduce delay
const delay = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));

/**
 * Wrapper for making GraphQL requests with retry logic.
 */
async function makeGraphQLRequest<T = any>(
  client: GraphQLClient,
  query: string,
  variables: any,
  maxRetries: number = MAX_RETRIES,
  delayMs: number = RETRY_DELAY_MS,
  queryName: string = "GraphQL" // For logging purposes
): Promise<T> {
  for (let i = 0; i < maxRetries; i++) {
    try {
      return await client.request<T>(query, variables);
    } catch (error: any) {
      console.error(`${queryName} request failed (attempt ${i + 1}/${maxRetries}): ${error.message}`);
      if (i === maxRetries - 1) {
        console.error(`All ${maxRetries} retries failed for ${queryName} query.`);
        throw error; 
      }
      console.log(`Waiting ${delayMs}ms before next retry...`);
      await delay(delayMs);
    }
  }
  throw new Error(`Failed to execute ${queryName} query after ${maxRetries} retries.`);
}


async function getPoolDataFromSDK(poolAddress: string, provider: ethers.providers.JsonRpcProvider) {
  // This function remains unchanged
  console.log("\n--- Fetching current pool data using Uniswap SDK ---");
  const poolContract = new ethers.Contract(poolAddress, IUniswapV3PoolABI, provider);
  try {
    const token0Address = await poolContract.token0();
    const token1Address = await poolContract.token1();
    const fee = await poolContract.fee();
    const liquidity = await poolContract.liquidity();
    const slot0 = await poolContract.slot0();
    const sqrtPriceX96 = slot0.sqrtPriceX96;
    const tickCurrent = slot0.tick;
    console.log(`Pool Address: ${poolAddress}`);
    console.log(`Token0: ${token0Address}`);
    console.log(`Token1: ${token1Address}`);
    console.log(`Fee: ${fee}`);
    console.log(`Current Liquidity: ${liquidity.toString()}`); // Liquidity is integer
    console.log(`Current SqrtPriceX96: ${sqrtPriceX96.toString()}`); // sqrtPriceX96 is integer
    console.log(`Current Tick: ${tickCurrent}`); // tick is integer
  } catch (error) {
    console.error("Error fetching pool data from SDK:", error);
  }
}

async function fetchHistoricalDataWithTheGraph(
  client: GraphQLClient, 
  poolAddress: string,
  startTimestamp: number,
  endTimestamp: number,
  swapsWriter: ObjectCsvWriter,
  mintsWriter: ObjectCsvWriter,
  burnsWriter: ObjectCsvWriter
): Promise<{ swapsCount: number, mintsCount: number, burnsCount: number }> {
  console.log(`\n--- Fetching historical data using The Graph for pool ${poolAddress} ---`);
  console.log(`Time range: ${new Date(startTimestamp * 1000).toISOString()} to ${new Date(endTimestamp * 1000).toISOString()}`);

  let allSwaps: SwapData[] = []; 
  let allMints: MintData[] = []; 
  let allBurns: BurnData[] = []; 

  // Fetch Swaps with timestamp-based pagination
  let lastTimestampSwaps = startTimestamp;
  let fetchMoreSwaps = true;
  console.log("\nFetching Swaps and writing incrementally...");
  while (fetchMoreSwaps) {
    const swapQuery = gql`
      query getSwaps($poolAddress: String!, $startTime: Int!, $endTime: Int!, $maxRecords: Int!) {
        swaps(
          first: $maxRecords
          where: {
            pool: $poolAddress
            timestamp_gte: $startTime
            timestamp_lt: $endTime 
          }
          orderBy: timestamp
          orderDirection: asc 
        ) {
          id
          timestamp
          token0 { id symbol }
          token1 { id symbol }
          sender
          recipient
          amount0 # Token amount, decimal string
          amount1 # Token amount, decimal string
          sqrtPriceX96
          tick
        }
      }
    `;
    try {
      const swapVariables = {
        poolAddress: poolAddress.toLowerCase(),
        startTime: lastTimestampSwaps,
        endTime: endTimestamp,
        maxRecords: MAX_RECORDS_PER_QUERY,
      };
      const data: any = await makeGraphQLRequest(client, swapQuery, swapVariables, MAX_RETRIES, RETRY_DELAY_MS, "Swaps");

      if (data.swaps && data.swaps.length > 0) {
        const processedSwapsBatch = data.swaps.map((s: any) => ({
          id: s.id,
          timestamp: s.timestamp,
          token0: s.token0.id,
          token1: s.token1.id,
          sender: s.sender,
          recipient: s.recipient,
          amount0: s.amount0, // Keep as string from The Graph
          amount1: s.amount1, // Keep as string from The Graph
          sqrtPriceX96: ethers.BigNumber.from(s.sqrtPriceX96).toString(), // Integer
          tick: ethers.BigNumber.from(s.tick).toString(), // Integer
        }));
        
        if (processedSwapsBatch.length > 0) {
            await swapsWriter.writeRecords(processedSwapsBatch);
            console.log(`Written ${processedSwapsBatch.length} swaps to CSV.`);
        }
        allSwaps = allSwaps.concat(processedSwapsBatch);

        const newLastTimestamp = parseInt(data.swaps[data.swaps.length - 1].timestamp);
        console.log(`Fetched ${data.swaps.length} swaps. Last timestamp: ${new Date(newLastTimestamp * 1000).toISOString()}. Total swaps so far: ${allSwaps.length}`);
        
        if (data.swaps.length < MAX_RECORDS_PER_QUERY) {
          fetchMoreSwaps = false; 
        } else {
          if (newLastTimestamp === lastTimestampSwaps) {
             console.warn("Swap Pagination: Timestamp hasn't changed but got a full batch. Incrementing timestamp by 1s to attempt to move forward.");
             lastTimestampSwaps = newLastTimestamp + 1; 
          } else {
            lastTimestampSwaps = newLastTimestamp;
          }
          await delay(BATCH_DELAY_MS); 
        }
      } else {
        fetchMoreSwaps = false; 
        console.log("No more swaps found in this iteration.");
      }
    } catch (error: any) {
      console.error("Failed to fetch swaps batch after multiple retries:", error.message);
      console.log("Stopping swap fetching due to persistent error.");
      fetchMoreSwaps = false; 
    }
  }
  console.log(`Total swaps fetched and written: ${allSwaps.length}`);

  // Fetch Mints with skip-based pagination
  let skipMints = 0;
  let fetchMoreMints = true;
  console.log("\nFetching Mints and writing incrementally...");
  while (fetchMoreMints) {
    const mintQuery = gql`
      query getMints($poolAddress: String!, $startTime: Int!, $endTime: Int!, $skip: Int!, $maxRecords: Int!) {
        mints(
          first: $maxRecords
          skip: $skip
          where: {
            pool: $poolAddress
            timestamp_gte: $startTime
            timestamp_lte: $endTime
          }
          orderBy: timestamp
          orderDirection: asc
        ) {
          id
          timestamp
          owner
          sender
          origin
          amount # Liquidity amount, integer
          amount0 # Token amount, decimal string
          amount1 # Token amount, decimal string
          tickLower
          tickUpper
        }
      }
    `;
    try {
      const mintVariables = {
        poolAddress: poolAddress.toLowerCase(),
        startTime: startTimestamp, 
        endTime: endTimestamp,
        skip: skipMints,
        maxRecords: MAX_RECORDS_PER_QUERY,
      };
      const data: any = await makeGraphQLRequest(client, mintQuery, mintVariables, MAX_RETRIES, RETRY_DELAY_MS, "Mints");
      
      if (data.mints && data.mints.length > 0) {
        const processedMintsBatch = data.mints.map((m: any) => ({
          id: m.id,
          timestamp: m.timestamp,
          owner: m.owner,
          sender: m.sender,
          origin: m.origin,
          amount: ethers.BigNumber.from(m.amount).toString(), // Liquidity, integer
          amount0: m.amount0, // Keep as string from The Graph
          amount1: m.amount1, // Keep as string from The Graph
          tickLower: ethers.BigNumber.from(m.tickLower).toString(), // Integer
          tickUpper: ethers.BigNumber.from(m.tickUpper).toString(), // Integer
        }));

        if (processedMintsBatch.length > 0) {
            await mintsWriter.writeRecords(processedMintsBatch);
            console.log(`Written ${processedMintsBatch.length} mints to CSV.`);
        }
        allMints = allMints.concat(processedMintsBatch); 

        console.log(`Fetched ${data.mints.length} mints. Total mints so far: ${allMints.length}. Current skip: ${skipMints}`);
        if (data.mints.length < MAX_RECORDS_PER_QUERY) {
          fetchMoreMints = false;
        } else {
          skipMints += MAX_RECORDS_PER_QUERY;
          await delay(BATCH_DELAY_MS); 
        }
      } else {
        fetchMoreMints = false; 
        console.log("No more mints found in this iteration.");
      }
    } catch (error: any) {
      console.error("Failed to fetch mints batch after multiple retries:", error.message);
      console.log("Stopping mint fetching due to persistent error.");
      fetchMoreMints = false; 
    }
  }
  console.log(`Total mints fetched and written: ${allMints.length}`);

  // Fetch Burns with skip-based pagination
  let skipBurns = 0;
  let fetchMoreBurns = true;
  console.log("\nFetching Burns and writing incrementally...");
  while (fetchMoreBurns) {
    const burnQuery = gql`
      query getBurns($poolAddress: String!, $startTime: Int!, $endTime: Int!, $skip: Int!, $maxRecords: Int!) {
        burns(
          first: $maxRecords
          skip: $skip
          where: {
            pool: $poolAddress
            timestamp_gte: $startTime
            timestamp_lte: $endTime
          }
          orderBy: timestamp
          orderDirection: asc
        ) {
          id
          timestamp
          owner
          origin
          amount # Liquidity amount, integer
          amount0 # Token amount, decimal string
          amount1 # Token amount, decimal string
          tickLower
          tickUpper
        }
      }
    `;
    try {
      const burnVariables = {
        poolAddress: poolAddress.toLowerCase(),
        startTime: startTimestamp, 
        endTime: endTimestamp,
        skip: skipBurns,
        maxRecords: MAX_RECORDS_PER_QUERY,
      };
      const data: any = await makeGraphQLRequest(client, burnQuery, burnVariables, MAX_RETRIES, RETRY_DELAY_MS, "Burns");

      if (data.burns && data.burns.length > 0) {
        const processedBurnsBatch = data.burns.map((b: any) => ({
          id: b.id,
          timestamp: b.timestamp,
          owner: b.owner,
          origin: b.origin,
          amount: ethers.BigNumber.from(b.amount).toString(), // Liquidity, integer
          amount0: b.amount0, // Keep as string from The Graph
          amount1: b.amount1, // Keep as string from The Graph
          tickLower: ethers.BigNumber.from(b.tickLower).toString(), // Integer
          tickUpper: ethers.BigNumber.from(b.tickUpper).toString(), // Integer
        }));
        
        if (processedBurnsBatch.length > 0) {
            await burnsWriter.writeRecords(processedBurnsBatch);
            console.log(`Written ${processedBurnsBatch.length} burns to CSV.`);
        }
        allBurns = allBurns.concat(processedBurnsBatch); 

        console.log(`Fetched ${data.burns.length} burns. Total burns so far: ${allBurns.length}. Current skip: ${skipBurns}`);
        if (data.burns.length < MAX_RECORDS_PER_QUERY) {
          fetchMoreBurns = false;
        } else {
          skipBurns += MAX_RECORDS_PER_QUERY;
          await delay(BATCH_DELAY_MS); 
        }
      } else {
        fetchMoreBurns = false; 
        console.log("No more burns found in this iteration.");
      }
    } catch (error: any) {
      console.error("Failed to fetch burns batch after multiple retries:", error.message);
      console.log("Stopping burn fetching due to persistent error.");
      fetchMoreBurns = false; 
    }
  }
  console.log(`Total burns fetched and written: ${allBurns.length}`);
  
  return { 
    swapsCount: allSwaps.length, 
    mintsCount: allMints.length, 
    burnsCount: allBurns.length 
  };
}

async function main() {
  if (INFURA_URL === 'YOUR_RPC_URL_HERE' || !INFURA_URL) {
    console.error("CRITICAL: INFURA_URL is not set. Please set it in the .env file or directly in the script.");
    process.exit(1);
  }
  if (!POOL_ADDRESS || (POOL_ADDRESS === '0x88e6A0c2dDD26FEEb64F039a2c41296FcB3f5640' && process.env.POOL_ADDRESS !== POOL_ADDRESS)) { 
     console.warn(`Using default POOL_ADDRESS: ${POOL_ADDRESS}. Ensure this is the pool you want or set POOL_ADDRESS in .env`);
  }

  const startTimestampStr = process.env.START_TIMESTAMP;
  const endTimestampStr = process.env.END_TIMESTAMP;

  if (!startTimestampStr || !endTimestampStr) {
    console.error(
      "CRITICAL: Both START_TIMESTAMP and END_TIMESTAMP must be set in the .env file. " +
      "Please provide them as Unix timestamps in seconds (e.g., START_TIMESTAMP=1672531200)."
    );
    process.exit(1);
  }

  const startTimestamp = parseInt(startTimestampStr, 10);
  const endTimestamp = parseInt(endTimestampStr, 10);

  if (isNaN(startTimestamp) || isNaN(endTimestamp)) {
    console.error(
      "CRITICAL: START_TIMESTAMP and END_TIMESTAMP must be valid numbers. " +
      "Please provide them as Unix timestamps in seconds (e.g., START_TIMESTAMP=1672531200)."
    );
    process.exit(1);
  }

  if (startTimestamp >= endTimestamp) {
    console.error("CRITICAL: START_TIMESTAMP must be earlier than END_TIMESTAMP.");
    process.exit(1);
  }
  
  console.log(`INFURA_URL detected: ${INFURA_URL.startsWith('YOUR_') ? 'Default/Example URL' : 'Custom URL set'}`);
  console.log(`POOL_ADDRESS: ${POOL_ADDRESS}`);

  const provider = new ethers.providers.JsonRpcProvider(INFURA_URL); 
  const graphQLClient = new GraphQLClient(THE_GRAPH_URL);

  // Initialize CSV writers
  const swapsFilePath = 'swaps.csv';
  const mintsFilePath = 'mints.csv';
  const burnsFilePath = 'burns.csv';

  const swapsWriter = createObjectCsvWriter({
    path: swapsFilePath,
    header: [
      { id: 'id', title: 'ID' },
      { id: 'timestamp', title: 'Timestamp' },
      { id: 'token0', title: 'Token0' },
      { id: 'token1', title: 'Token1' },
      { id: 'sender', title: 'Sender' },
      { id: 'recipient', title: 'Recipient' },
      { id: 'amount0', title: 'Amount0' },
      { id: 'amount1', title: 'Amount1' },
      { id: 'sqrtPriceX96', title: 'SqrtPriceX96' },
      { id: 'tick', title: 'Tick' },
    ]
  });

  const mintsWriter = createObjectCsvWriter({
    path: mintsFilePath,
    header: [
      { id: 'id', title: 'ID' },
      { id: 'timestamp', title: 'Timestamp' },
      { id: 'owner', title: 'Owner' },
      { id: 'sender', title: 'Sender' },
      { id: 'origin', title: 'Origin' },
      { id: 'amount', title: 'Amount' },
      { id: 'amount0', title: 'Amount0' },
      { id: 'amount1', title: 'Amount1' },
      { id: 'tickLower', title: 'TickLower' },
      { id: 'tickUpper', title: 'TickUpper' },
    ]
  });

  const burnsWriter = createObjectCsvWriter({
    path: burnsFilePath,
    header: [
      { id: 'id', title: 'ID' },
      { id: 'timestamp', title: 'Timestamp' },
      { id: 'owner', title: 'Owner' },
      { id: 'origin', title: 'Origin' },
      { id: 'amount', title: 'Amount' },
      { id: 'amount0', title: 'Amount0' },
      { id: 'amount1', title: 'Amount1' },
      { id: 'tickLower', title: 'TickLower' },
      { id: 'tickUpper', title: 'TickUpper' },
    ]
  });
  
  console.log(`Output files will be: ${swapsFilePath}, ${mintsFilePath}, ${burnsFilePath}`);
  console.log("IMPORTANT: Existing files with these names will be overwritten.");


  // Optional: Call the function to get current pool data via SDK
  // await getPoolDataFromSDK(POOL_ADDRESS, provider);

  try {
    const { swapsCount, mintsCount, burnsCount } = await fetchHistoricalDataWithTheGraph(
      graphQLClient, 
      POOL_ADDRESS,
      startTimestamp,
      endTimestamp,
      swapsWriter,
      mintsWriter,
      burnsWriter
    );

    console.log("\n--- Data fetching and incremental writing complete ---");
    console.log(`Total swaps written: ${swapsCount}`);
    console.log(`Total mints written: ${mintsCount}`);
    console.log(`Total burns written: ${burnsCount}`);

    if (swapsCount === 0 && mintsCount === 0 && burnsCount === 0) {
        console.warn("Warning: No data was fetched for swaps, mints, or burns in the specified time range. Check pool activity and timestamps.");
    }

  } catch (error: any) {
     console.error("CRITICAL ERROR in main execution after retries:", error.message);
     process.exitCode = 1;
  }
}

main().catch((error) => { 
  console.error("Unhandled CRITICAL ERROR in main execution:", error);
  process.exitCode = 1;
});
