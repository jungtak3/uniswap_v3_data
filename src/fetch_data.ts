import { ethers } from 'ethers';
import { Pool, TickListDataProvider, Tick } from '@uniswap/v3-sdk';
import { GraphQLClient, gql } from 'graphql-request';
import { createObjectCsvWriter } from 'csv-writer';
import { CsvWriter } from 'csv-writer/src/lib/csv-writer'; // For typing
import * as dotenv from 'dotenv';

// Initialize dotenv to load environment variables
dotenv.config();

// Configuration constants/variables from environment
const INFURA_URL = process.env.INFURA_URL ||'https://mainnet.infura.io/v3/97574cc27eba4c56ae3ae8937f706131' ;
const POOL_ADDRESS = process.env.POOL_ADDRESS || '0x8ad599c3a0ff1de082011efddc58f1908eb6e6d8';
const THE_GRAPH_URL = process.env.THE_GRAPH_URL || 'https://gateway.thegraph.com/eb7648b32137aed8efb0a31da11ed06a/subgraphs/id/5zvR82QoaXYFyDEKLZ9t6v9adgnptxYpKpSbxtgVENFV'; 
const MAX_RECORDS_PER_QUERY = 100;
const BUCKET_INTERVAL_SECONDS = 3600; // 1 hour in seconds
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

// Minimal ERC20 ABI for fetching decimals
const ERC20_ABI_MINIMAL = [
  "function decimals() external view returns (uint8)"
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

// Interface for individual price points, used for OHLC calculation
interface PriceData {
  timestamp: number;
  price: number;
}

// Interface for OHLC data
interface OHLCData {
  timestamp: number; // Start timestamp of the bucket
  open: number;
  high: number;
  low: number;
  close: number;
}

// Interface for Liquidity Metric data
interface LiquidityMetric {
  timestamp: number; // Corresponds to OHLCData timestamp for alignment
  activeLiquidityInRange: string; // BigNumber string
  totalLiquidityInPool: string;   // BigNumber string
  ratio: number;                  // active / total
}

// Interface for the final combined historical data
interface FinalHistoricalData {
  timestamp: number;
  open: number;
  high: number;
  low: number;
  close: number;
  activeLiquidityInRange: string;
  totalLiquidityInPool: string;
  liquidityRatio: number;
}

// Define a type for our CSV writer instances for better type safety
type ObjectCsvWriter = CsvWriter<object>;


// Helper function to introduce delay
const delay = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));

/**
 * Converts a sqrtPriceX96 value to a human-readable price.
 * Price is token1 in terms of token0 (e.g., WETH price in USDC if token0 is USDC, token1 is WETH).
 * @param sqrtPriceX96 BigNumber representation of sqrtPriceX96 from Uniswap.
 * @param token0Decimals Decimals of token0.
 * @param token1Decimals Decimals of token1.
 * @returns The calculated price as a number.
 */
function sqrtPriceX96ToPrice(
  sqrtPriceX96: ethers.BigNumber,
  token0Decimals: number,
  token1Decimals: number
): number {
  // (sqrtPriceX96 / 2^96)^2 * (10^token0Decimals / 10^token1Decimals)
  // sqrtPriceX96 is Q64.96, so dividing by 2^96 gives the price ratio.
  // Then square it to get price.
  // Then adjust for decimals.

  const Q96 = ethers.BigNumber.from(2).pow(96);

  // Calculate (sqrtPriceX96 / 2^96)^2
  // To maintain precision, square sqrtPriceX96 first, then divide by (2^96)^2 = 2^192
  const priceRatioX192 = sqrtPriceX96.pow(2); // This is now (sqrtPriceX96)^2
  const Q192 = ethers.BigNumber.from(2).pow(192); // This is (2^96)^2

  // Now, incorporate decimal adjustment: (10^token0Decimals / 10^token1Decimals)
  // price = (priceRatioX192 / Q192) * (10^token0Decimals / 10^token1Decimals)
  // price = (priceRatioX192 * 10^token0Decimals) / (Q192 * 10^token1Decimals)

  const numerator = priceRatioX192.mul(ethers.BigNumber.from(10).pow(token0Decimals));
  const denominator = Q192.mul(ethers.BigNumber.from(10).pow(token1Decimals));

  if (denominator.isZero()) {
    console.warn("Denominator is zero in sqrtPriceX96ToPrice calculation. This should not happen with valid pool data.");
    return 0; // Or throw an error, depending on desired handling
  }

  // Perform division. To get a float, we need to convert to string or use formatUnits.
  // Using formatUnits with a high number of decimals (e.g., 18 or more) for the intermediate division,
  // then parseFloat. The "unit" for formatUnits here is effectively the precision we want for the division.
  // Let's assume we want the price to be human-readable, so we format it to a reasonable number of effective decimals.
  // The number of decimals in the result of numerator.div(denominator) is effectively 0.
  // We need to scale the numerator before division to preserve decimals.
  
  // Let's scale the numerator by a large factor (e.g., 10^18) before division,
  // then parse the result as a float and divide by that factor.
  const scalingFactor = ethers.BigNumber.from(10).pow(18); // For 18 decimal places of precision in the division
  const scaledNumerator = numerator.mul(scalingFactor);
  
  const priceBigNumber = scaledNumerator.div(denominator);
  
  return parseFloat(ethers.utils.formatUnits(priceBigNumber, 18)); // Convert back after scaling
}

/**
 * Determines the tick spacing for a Uniswap V3 pool based on its fee tier.
 * @param fee The fee tier of the pool (e.g., 100, 500, 3000, 10000).
 * @returns The corresponding tick spacing.
 * @throws Error if the fee tier is unknown.
 */
function getTickSpacingFromFee(fee: number): number {
  switch (fee) {
    case 100: // 0.01%
      return 1;
    case 500: // 0.05%
      return 10;
    case 3000: // 0.30%
      return 60;
    case 10000: // 1.00%
      return 200;
    default:
      throw new Error(`Unknown fee: ${fee}, cannot determine tickSpacing. Supported fees are 100, 500, 3000, 10000.`);
  }
}

/**
 * Converts a human-readable price to the nearest valid Uniswap V3 tick.
 * @param price Price of token1 in terms of token0.
 * @param token0Decimals Decimals of token0.
 * @param token1Decimals Decimals of token1.
 * @param tickSpacing The tick spacing of the pool.
 * @returns The nearest valid tick index.
 */
function priceToTick(
  price: number,
  token0Decimals: number,
  token1Decimals: number,
  tickSpacing: number
): number {
  if (price <= 0) {
    // Logarithm of non-positive number is undefined.
    // Depending on how price is derived, this might indicate an issue or edge case.
    // Return MIN_TICK or throw an error, based on desired handling.
    // For now, let's assume price is always positive.
    // Uniswap SDK uses MIN_TICK = -887272 and MAX_TICK = 887272
    console.warn("priceToTick received a non-positive price. This might lead to unexpected tick values.");
    // A very small positive price will result in a very negative tick.
    // Let's clamp to a representable range if needed, though the formula itself should handle typical price ranges.
  }

  // Effective price considering decimals: price * 10^(token1Decimals - token0Decimals)
  // This is price of token1 in terms of token0, adjusted for decimal differences.
  // Example: if token0 is USDC (6 decimals) and token1 is WETH (18 decimals),
  // price = WETH_PRICE_IN_USDC.
  // effectivePrice = WETH_PRICE_IN_USDC * 10^(18-6) = WETH_PRICE_IN_USDC * 10^12
  const effectivePrice = price * (10 ** (token1Decimals - token0Decimals));

  // tick = log_sqrt(1.0001)(effective_price)
  // log_base(value) = Math.log(value) / Math.log(base)
  // base = sqrt(1.0001) = 1.00005
  const rawTick = Math.log(effectivePrice) / Math.log(1.00005);

  // Round to the nearest multiple of tickSpacing
  // Math.round(value / interval) * interval
  const roundedTick = Math.round(rawTick / tickSpacing) * tickSpacing;
  
  // Uniswap SDK TickMath defines MIN_TICK and MAX_TICK
  // const MIN_TICK = -887272;
  // const MAX_TICK = 887272;
  // return Math.max(MIN_TICK, Math.min(MAX_TICK, roundedTick));
  // For now, not clamping to global MIN/MAX_TICK unless it becomes an issue.
  return roundedTick;
}


/**
 * Calculates OHLC (Open, High, Low, Close) data from swap data.
 * @param swaps Array of SwapData, expected to be sorted by timestamp.
 * @param bucketIntervalSeconds The duration of each OHLC bucket in seconds.
 * @param token0Decimals Decimals of token0.
 * @param token1Decimals Decimals of token1.
 * @returns An array of OHLCData.
 */
function calculateOHLC(
  swaps: SwapData[],
  bucketIntervalSeconds: number,
  token0Decimals: number,
  token1Decimals: number
): OHLCData[] {
  const ohlcDataArray: OHLCData[] = [];
  if (swaps.length === 0) {
    return ohlcDataArray;
  }

  // Ensure swaps are sorted by timestamp (The Graph usually does this, but defensive check)
  // For simplicity, this example assumes swaps are pre-sorted. 
  // If not, sort them here: swaps.sort((a, b) => parseInt(a.timestamp) - parseInt(b.timestamp));

  let currentBucketStartTimestamp = Math.floor(parseInt(swaps[0].timestamp) / bucketIntervalSeconds) * bucketIntervalSeconds;
  let bucketPrices: number[] = []; // Store prices directly for easier Math.max/min

  for (const swap of swaps) {
    const swapTimestamp = parseInt(swap.timestamp);
    const swapPrice = sqrtPriceX96ToPrice(ethers.BigNumber.from(swap.sqrtPriceX96), token0Decimals, token1Decimals);

    if (swapTimestamp < currentBucketStartTimestamp + bucketIntervalSeconds) {
      bucketPrices.push(swapPrice);
    } else {
      // Process the completed bucket
      if (bucketPrices.length > 0) {
        ohlcDataArray.push({
          timestamp: currentBucketStartTimestamp,
          open: bucketPrices[0],
          high: Math.max(...bucketPrices),
          low: Math.min(...bucketPrices),
          close: bucketPrices[bucketPrices.length - 1],
        });
      }
      // Reset for new bucket
      currentBucketStartTimestamp = Math.floor(swapTimestamp / bucketIntervalSeconds) * bucketIntervalSeconds;
      bucketPrices = [swapPrice];
    }
  }

  // Process the last bucket after the loop
  if (bucketPrices.length > 0) {
    ohlcDataArray.push({
      timestamp: currentBucketStartTimestamp,
      open: bucketPrices[0],
      high: Math.max(...bucketPrices),
      low: Math.min(...bucketPrices),
      close: bucketPrices[bucketPrices.length - 1],
    });
  }

  return ohlcDataArray;
}

/**
 * Calculates liquidity metrics for each OHLC bucket.
 * @param ohlcRecords Array of OHLCData.
 * @param allMints Array of MintData.
 * @param allBurns Array of BurnData.
 * @param token0Decimals Decimals of token0.
 * @param token1Decimals Decimals of token1.
 * @param tickSpacing Tick spacing of the pool.
 * @param bucketIntervalSeconds Duration of each OHLC bucket.
 * @returns An array of LiquidityMetric.
 */
function calculateLiquidityMetrics(
  ohlcRecords: OHLCData[],
  allMints: MintData[],
  allBurns: BurnData[],
  token0Decimals: number,
  token1Decimals: number,
  tickSpacing: number,
  bucketIntervalSeconds: number
): LiquidityMetric[] {
  const liquidityMetrics: LiquidityMetric[] = [];
  if (ohlcRecords.length === 0) {
    return liquidityMetrics;
  }

  // Sort mints and burns by timestamp to ensure chronological processing
  // The Graph usually returns sorted data, but this is a safeguard.
  const sortedMints = [...allMints].sort((a, b) => parseInt(a.timestamp) - parseInt(b.timestamp));
  const sortedBurns = [...allBurns].sort((a, b) => parseInt(a.timestamp) - parseInt(b.timestamp));

  const tickLiquidityMap = new Map<number, ethers.BigNumber>();
  let currentTotalLiquidityInPool = ethers.BigNumber.from(0);
  let mintIdx = 0;
  let burnIdx = 0;

  for (const ohlcRecord of ohlcRecords) {
    const bucketStartTime = ohlcRecord.timestamp;
    const bucketEndTime = bucketStartTime + bucketIntervalSeconds;

    // Process mints that occurred up to the end of the current OHLC bucket
    while (mintIdx < sortedMints.length && parseInt(sortedMints[mintIdx].timestamp) < bucketEndTime) {
      const mint = sortedMints[mintIdx];
      const liquidityAmount = ethers.BigNumber.from(mint.amount);
      currentTotalLiquidityInPool = currentTotalLiquidityInPool.add(liquidityAmount);
      const tickLower = parseInt(mint.tickLower);
      const tickUpper = parseInt(mint.tickUpper);

      // Add liquidity to each tick in the range [tickLower, tickUpper)
      for (let t = tickLower; t < tickUpper; t += 1) { // Iterate over every discrete tick
        // It's important that tickLower and tickUpper from mints/burns are multiples of tickSpacing
        // or aligned with how ticks are generally used. The Graph provides these directly.
        tickLiquidityMap.set(t, (tickLiquidityMap.get(t) || ethers.BigNumber.from(0)).add(liquidityAmount));
      }
      mintIdx++;
    }

    // Process burns that occurred up to the end of the current OHLC bucket
    while (burnIdx < sortedBurns.length && parseInt(sortedBurns[burnIdx].timestamp) < bucketEndTime) {
      const burn = sortedBurns[burnIdx];
      const liquidityAmount = ethers.BigNumber.from(burn.amount);
      currentTotalLiquidityInPool = currentTotalLiquidityInPool.sub(liquidityAmount);
      // Ensure total liquidity doesn't go negative due to data inconsistencies or precision issues
      if (currentTotalLiquidityInPool.lt(0)) currentTotalLiquidityInPool = ethers.BigNumber.from(0); 
      
      const tickLower = parseInt(burn.tickLower);
      const tickUpper = parseInt(burn.tickUpper);

      // Subtract liquidity from each tick in the range [tickLower, tickUpper)
      for (let t = tickLower; t < tickUpper; t += 1) { // Iterate over every discrete tick
        const currentTickLiq = tickLiquidityMap.get(t) || ethers.BigNumber.from(0);
        let newTickLiq = currentTickLiq.sub(liquidityAmount);
        if (newTickLiq.lt(0)) {
            // This might happen if burn events are processed for liquidity that wasn't tracked (e.g. outside initial state)
            // Or if there are more burns than mints for a tick in the dataset.
            // console.warn(`Negative liquidity calculated for tick ${t}. Clamping to 0. Burn ID: ${burn.id}`);
            newTickLiq = ethers.BigNumber.from(0);
        }
        tickLiquidityMap.set(t, newTickLiq);
      }
      burnIdx++;
    }

    // Calculate active liquidity in the OHLC price range (low to high)
    // The priceToTick function already rounds to the nearest tickSpacing multiple.
    const lowTickRange = priceToTick(ohlcRecord.low, token0Decimals, token1Decimals, tickSpacing);
    const highTickRange = priceToTick(ohlcRecord.high, token0Decimals, token1Decimals, tickSpacing);
    
    // Ensure lowTick <= highTick. If high < low (e.g. due to price volatility and tick rounding), swap them.
    const finalLowTick = Math.min(lowTickRange, highTickRange);
    const finalHighTick = Math.max(lowTickRange, highTickRange);

    let activeLiquidity = ethers.BigNumber.from(0);
    // Sum liquidity for ticks within the [finalLowTick, finalHighTick) range.
    // Iterate using tickSpacing to sum up liquidity across the relevant tick intervals.
    // However, tickLiquidityMap stores liquidity per *discrete* tick.
    // The active liquidity for a range [T_low, T_high) is the sum of L_i for each tick i in that range.
    for (let t = finalLowTick; t < finalHighTick; t += 1) { // Iterate over every discrete tick
        activeLiquidity = activeLiquidity.add(tickLiquidityMap.get(t) || ethers.BigNumber.from(0));
    }
    
    const ratio = currentTotalLiquidityInPool.isZero() || activeLiquidity.isZero()
      ? 0
      : parseFloat(ethers.utils.formatUnits(activeLiquidity.mul(ethers.BigNumber.from(10).pow(18)).div(currentTotalLiquidityInPool), 18));

    liquidityMetrics.push({
      timestamp: ohlcRecord.timestamp,
      activeLiquidityInRange: activeLiquidity.toString(),
      totalLiquidityInPool: currentTotalLiquidityInPool.toString(),
      ratio: ratio,
    });
  }

  return liquidityMetrics;
}


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
  endTimestamp: number
): Promise<{ allSwaps: SwapData[], allMints: MintData[], allBurns: BurnData[] }> {
  console.log(`\n--- Fetching historical data into memory using The Graph for pool ${poolAddress} ---`);
  console.log(`Time range: ${new Date(startTimestamp * 1000).toISOString()} to ${new Date(endTimestamp * 1000).toISOString()}`);

  let allSwaps: SwapData[] = []; 
  let allMints: MintData[] = []; 
  let allBurns: BurnData[] = []; 

  // Fetch Swaps with timestamp-based pagination
  let lastTimestampSwaps = startTimestamp;
  let fetchMoreSwaps = true;
  console.log("\nFetching Swaps into memory...");
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
        allSwaps = allSwaps.concat(processedSwapsBatch);

        const newLastTimestamp = parseInt(data.swaps[data.swaps.length - 1].timestamp);
        console.log(`Fetched ${data.swaps.length} swaps batch. Last timestamp: ${new Date(newLastTimestamp * 1000).toISOString()}. Total swaps in memory: ${allSwaps.length}`);
        
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
  console.log(`Total swaps fetched into memory: ${allSwaps.length}`);

  // Fetch Mints with skip-based pagination
  let skipMints = 0;
  let fetchMoreMints = true;
  console.log("\nFetching Mints into memory...");
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

        allMints = allMints.concat(processedMintsBatch);

        console.log(`Fetched ${data.mints.length} mints batch. Total mints in memory: ${allMints.length}. Current skip: ${skipMints}`);
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
  console.log(`Total mints fetched into memory: ${allMints.length}`);

  // Fetch Burns with skip-based pagination
  let skipBurns = 0;
  let fetchMoreBurns = true;
  console.log("\nFetching Burns into memory...");
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
        allBurns = allBurns.concat(processedBurnsBatch);

        console.log(`Fetched ${data.burns.length} burns batch. Total burns in memory: ${allBurns.length}. Current skip: ${skipBurns}`);
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
  console.log(`Total burns fetched into memory: ${allBurns.length}`);
  
  return { allSwaps, allMints, allBurns };
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

  // Fetch token decimals
  let token0Address: string;
  let token1Address: string;
  let token0Decimals: number;
  let token1Decimals: number;
  let fee: number;
  let tickSpacing: number;

  try {
    const poolContract = new ethers.Contract(POOL_ADDRESS, IUniswapV3PoolABI, provider);
    token0Address = await poolContract.token0();
    token1Address = await poolContract.token1();
    console.log(`Token0 Address: ${token0Address}`);
    console.log(`Token1 Address: ${token1Address}`);

    const token0Contract = new ethers.Contract(token0Address, ERC20_ABI_MINIMAL, provider);
    const token1Contract = new ethers.Contract(token1Address, ERC20_ABI_MINIMAL, provider);

    token0Decimals = await token0Contract.decimals();
    token1Decimals = await token1Contract.decimals();
    console.log(`Token0 Decimals: ${token0Decimals}`);
    console.log(`Token1 Decimals: ${token1Decimals}`);

    fee = await poolContract.fee();
    console.log(`Pool Fee: ${fee}`);
    tickSpacing = getTickSpacingFromFee(fee);
    console.log(`Tick Spacing: ${tickSpacing}`);

  } catch (error) {
    console.error("CRITICAL: Failed to fetch token details, fee, or determine tick spacing.", error);
    process.exit(1); // Exit if we can't get critical data
  }

  // Comment out or remove CSV writer initializations for individual files
  // const swapsFilePath = 'swaps.csv';
  // const mintsFilePath = 'mints.csv';
  // const burnsFilePath = 'burns.csv';

  // const swapsWriter = createObjectCsvWriter({
  //   path: swapsFilePath,
  //   header: [
  //     { id: 'id', title: 'ID' },
  //     { id: 'timestamp', title: 'Timestamp' },
  //     { id: 'token0', title: 'Token0' },
  //     { id: 'token1', title: 'Token1' },
  //     { id: 'sender', title: 'Sender' },
  //     { id: 'recipient', title: 'Recipient' },
  //     { id: 'amount0', title: 'Amount0' },
  //     { id: 'amount1', title: 'Amount1' },
  //     { id: 'sqrtPriceX96', title: 'SqrtPriceX96' },
  //     { id: 'tick', title: 'Tick' },
  //   ]
  // });

  // const mintsWriter = createObjectCsvWriter({
  //   path: mintsFilePath,
  //   header: [
  //     { id: 'id', title: 'ID' },
  //     { id: 'timestamp', title: 'Timestamp' },
  //     { id: 'owner', title: 'Owner' },
  //     { id: 'sender', title: 'Sender' },
  //     { id: 'origin', title: 'Origin' },
  //     { id: 'amount', title: 'Amount' },
  //     { id: 'amount0', title: 'Amount0' },
  //     { id: 'amount1', title: 'Amount1' },
  //     { id: 'tickLower', title: 'TickLower' },
  //     { id: 'tickUpper', title: 'TickUpper' },
  //   ]
  // });

  // const burnsWriter = createObjectCsvWriter({
  //   path: burnsFilePath,
  //   header: [
  //     { id: 'id', title: 'ID' },
  //     { id: 'timestamp', title: 'Timestamp' },
  //     { id: 'owner', title: 'Owner' },
  //     { id: 'origin', title: 'Origin' },
  //     { id: 'amount', title: 'Amount' },
  //     { id: 'amount0', title: 'Amount0' },
  //     { id: 'amount1', title: 'Amount1' },
  //     { id: 'tickLower', title: 'TickLower' },
  //     { id: 'tickUpper', title: 'TickUpper' },
  //   ]
  // });
  
  // console.log(`Output files will be: ${swapsFilePath}, ${mintsFilePath}, ${burnsFilePath}`);
  // console.log("IMPORTANT: Existing files with these names will be overwritten.");
  console.log("Data will be fetched into memory, not written to individual CSV files directly by fetchHistoricalDataWithTheGraph.");


  // Optional: Call the function to get current pool data via SDK
  // await getPoolDataFromSDK(POOL_ADDRESS, provider);

  try {
    const { allSwaps, allMints, allBurns } = await fetchHistoricalDataWithTheGraph(
      graphQLClient, 
      POOL_ADDRESS,
      startTimestamp,
      endTimestamp
    );

    console.log("\n--- Data fetching into memory complete ---");
    console.log(`Total swaps fetched: ${allSwaps.length}`);
    console.log(`Total mints fetched: ${allMints.length}`);
    console.log(`Total burns fetched: ${allBurns.length}`);

    // Temporary logs to verify counts
    console.log(`Verification: Swaps array length: ${allSwaps.length}`);
    console.log(`Verification: Mints array length: ${allMints.length}`);
    console.log(`Verification: Burns array length: ${allBurns.length}`);

    if (allSwaps.length === 0 && allMints.length === 0 && allBurns.length === 0) {
        console.warn("Warning: No data was fetched for swaps, mints, or burns in the specified time range. Check pool activity and timestamps.");
    }

    let ohlcRecords: OHLCData[] = []; // Define here to be in scope for liquidity metrics
    // Calculate OHLC data if swaps are available
    if (allSwaps.length > 0) {
      console.log("\n--- Calculating OHLC data ---");
      ohlcRecords = calculateOHLC(allSwaps, BUCKET_INTERVAL_SECONDS, token0Decimals, token1Decimals);
      console.log(`Calculated ${ohlcRecords.length} OHLC records.`);
      // Log the first few OHLC records for verification
      console.log("First 5 OHLC records:", ohlcRecords.slice(0, 5));

      if (ohlcRecords.length === 0 && allSwaps.length > 0) {
          console.warn("Warning: OHLC calculation resulted in zero records, but swaps were present. Check bucket interval or logic.");
      }
    } else {
      console.log("\nNo swaps data available to calculate OHLC.");
    }

    let liquidityMetrics: LiquidityMetric[] = []; // Define here to be in scope for combining
    // Calculate Liquidity Metrics if OHLC records are available
    if (ohlcRecords.length > 0) { // Check ohlcRecords directly
      console.log("\n--- Calculating Liquidity Metrics ---");
      liquidityMetrics = calculateLiquidityMetrics(
        ohlcRecords,
        allMints,
        allBurns,
        token0Decimals,
        token1Decimals,
        tickSpacing, // Fetched and logged earlier
        BUCKET_INTERVAL_SECONDS
      );
      console.log(`Calculated ${liquidityMetrics.length} liquidity metric records.`);
      // Log the first few liquidity metric records for verification
      console.log("First 5 Liquidity Metric records:", liquidityMetrics.slice(0, 5));

    } else {
      console.log("\nNo OHLC records available to calculate liquidity metrics.");
    }

    // Combine OHLC and Liquidity Metric data and write to CSV
    if (ohlcRecords.length > 0 && liquidityMetrics.length > 0) {
      if (ohlcRecords.length !== liquidityMetrics.length) {
        console.error(
          "CRITICAL: Mismatch between OHLC records and Liquidity records length. " +
          `OHLC: ${ohlcRecords.length}, Liquidity: ${liquidityMetrics.length}. Cannot combine and write to CSV.`
        );
        // Optionally, process.exit(1) here if this is considered a fatal error for the script's purpose
      } else {
        console.log("\n--- Combining OHLC and Liquidity Data ---");
        const combinedData: FinalHistoricalData[] = [];
        for (let i = 0; i < ohlcRecords.length; i++) {
          const ohlc = ohlcRecords[i];
          const metric = liquidityMetrics[i];

          if (ohlc.timestamp !== metric.timestamp) {
            console.warn(
              `Timestamp mismatch at index ${i}: OHLC ts ${ohlc.timestamp}, Metric ts ${metric.timestamp}. ` +
              `Using OHLC timestamp. Data for this entry might be misaligned.`
            );
            // Depending on strictness, one might choose to skip this record or throw an error.
            // For now, we proceed using the OHLC timestamp.
          }

          combinedData.push({
            timestamp: ohlc.timestamp,
            open: ohlc.open,
            high: ohlc.high,
            low: ohlc.low,
            close: ohlc.close,
            activeLiquidityInRange: metric.activeLiquidityInRange,
            totalLiquidityInPool: metric.totalLiquidityInPool,
            liquidityRatio: metric.ratio,
          });
        }
        console.log(`Generated ${combinedData.length} combined historical data records.`);

        if (combinedData.length > 0) {
          const finalCsvPath = 'historical_pool_data.csv';
          const finalCsvWriter = createObjectCsvWriter({
            path: finalCsvPath,
            header: [
              { id: 'timestamp', title: 'Time' },
              { id: 'open', title: 'Open' },
              { id: 'high', title: 'High' },
              { id: 'low', title: 'Low' },
              { id: 'close', title: 'Close' },
              { id: 'activeLiquidityInRange', title: 'ActiveLiquidityInRange' },
              { id: 'totalLiquidityInPool', title: 'TotalLiquidityInPool' },
              { id: 'liquidityRatio', title: 'LiquidityRatio_ActiveInRange_vs_TotalInPool' }
            ]
          });

          try {
            await finalCsvWriter.writeRecords(combinedData);
            console.log(`\n--- Final historical data successfully written to ${finalCsvPath} ---`);
            console.log(`Total records written: ${combinedData.length}`);
          } catch (writeError) {
            console.error("CRITICAL: Failed to write combined historical data to CSV.", writeError);
          }
        } else {
          console.log("No combined data was generated to write to CSV.");
        }
      }
    } else {
      console.log("\nNot enough data to combine and write to final CSV (OHLC or Liquidity metrics missing).");
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
