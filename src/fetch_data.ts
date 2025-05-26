import { ethers } from 'ethers';
import { Pool, TickListDataProvider, Tick } from '@uniswap/v3-sdk';
import { GraphQLClient, gql } from 'graphql-request';
import { createObjectCsvWriter } from 'csv-writer';

// Configuration constants/variables
const INFURA_URL = 'https://mainnet.infura.io/v3/97574cc27eba4c56ae3ae8937f706131'; // Replace with your Infura or Alchemy URL
const POOL_ADDRESS = '0x88e6A0c2dDD26FEEb64F039a2c41296FcB3f5640'; // Example: USDC/WETH 0.05% pool
const THE_GRAPH_URL = 'https://gateway.thegraph.com/api/eb7648b32137aed8efb0a31da11ed06a/subgraphs/id/5zvR82QoaXYFyDEKLZ9t6v9adgnptxYpKpSbxtgVENFV';
const MAX_RECORDS_PER_QUERY = 1000; // The Graph typically limits to 100 or 1000

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
  amount: string;
  amount0: string;
  amount1: string;
  tickLower: string;
  tickUpper: string;
}

interface BurnData {
  id: string;
  timestamp: string;
  owner: string;
  origin: string;
  amount: string;
  amount0: string;
  amount1: string;
  tickLower: string;
  tickUpper: string;
}


async function getPoolDataFromSDK(poolAddress: string, provider: ethers.providers.JsonRpcProvider) {
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
    console.log(`Current Liquidity: ${liquidity.toString()}`);
    console.log(`Current SqrtPriceX96: ${sqrtPriceX96.toString()}`);
    console.log(`Current Tick: ${tickCurrent}`);

  } catch (error) {
    console.error("Error fetching pool data from SDK:", error);
  }
}

async function fetchHistoricalDataWithTheGraph(
  poolAddress: string,
  client: GraphQLClient,
  startTimestamp: number,
  endTimestamp: number
): Promise<{ swaps: SwapData[], mints: MintData[], burns: BurnData[] }> {
  console.log(`\n--- Fetching historical data using The Graph for pool ${poolAddress} ---`);
  console.log(`Time range: ${new Date(startTimestamp * 1000).toISOString()} to ${new Date(endTimestamp * 1000).toISOString()}`);

  let allSwaps: SwapData[] = [];
  let allMints: MintData[] = [];
  let allBurns: BurnData[] = [];

  let lastTimestampSwaps = startTimestamp;
  let lastTimestampMints = startTimestamp;
  let lastTimestampBurns = startTimestamp;
  let fetchMoreSwaps = true;
  // For simplicity, pagination is only fully implemented for swaps in this example.
  // Mints and Burns will fetch only the first batch.

  // Fetch Swaps with pagination
  console.log("\nFetching Swaps...");
  while (fetchMoreSwaps) {
    const swapQuery = gql`
      query getSwaps($poolAddress: String!, $startTime: Int!, $endTime: Int!, $maxRecords: Int!) {
        swaps(
          first: $maxRecords
          where: {
            pool: $poolAddress
            timestamp_gte: $startTime
            timestamp_lt: $endTime # Use _lt for exclusive end to avoid overlap in pagination
          }
          orderBy: timestamp
          orderDirection: asc # Fetch oldest first to paginate correctly
        ) {
          id
          timestamp
          token0 { id symbol }
          token1 { id symbol }
          sender
          recipient
          amount0
          amount1
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
      // console.log("Querying swaps with variables:", swapVariables);
      const data: any = await client.request(swapQuery, swapVariables);

      if (data.swaps && data.swaps.length > 0) {
        const processedSwaps = data.swaps.map((s: any) => ({
          id: s.id,
          timestamp: s.timestamp,
          token0: s.token0.id,
          token1: s.token1.id,
          sender: s.sender,
          recipient: s.recipient,
          amount0: ethers.BigNumber.from(s.amount0).toString(),
          amount1: ethers.BigNumber.from(s.amount1).toString(),
          sqrtPriceX96: ethers.BigNumber.from(s.sqrtPriceX96).toString(),
          tick: ethers.BigNumber.from(s.tick).toString(),
        }));
        allSwaps = allSwaps.concat(processedSwaps);
        lastTimestampSwaps = parseInt(data.swaps[data.swaps.length - 1].timestamp);
        console.log(`Fetched ${data.swaps.length} swaps. Last timestamp: ${new Date(lastTimestampSwaps * 1000).toISOString()}. Total swaps: ${allSwaps.length}`);
        if (data.swaps.length < MAX_RECORDS_PER_QUERY) {
          fetchMoreSwaps = false; // No more data to fetch for swaps
        } else {
           // Small increment to avoid fetching the same last record if timestamps are identical
          lastTimestampSwaps +=1;
        }
      } else {
        fetchMoreSwaps = false; // No more data
      }
    } catch (error) {
      console.error("Error fetching swaps from The Graph:", error);
      fetchMoreSwaps = false; // Stop fetching on error
    }
  }
  console.log(`Total swaps fetched: ${allSwaps.length}`);

  // Fetch Mints (first batch only for this example)
  console.log("\nFetching Mints (first batch)...");
  const mintQuery = gql`
    query getMints($poolAddress: String!, $startTime: Int!, $endTime: Int!, $maxRecords: Int!) {
      mints(
        first: $maxRecords
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
        amount
        amount0
        amount1
        tickLower
        tickUpper
      }
    }
  `;
  try {
    const mintVariables = {
      poolAddress: poolAddress.toLowerCase(),
      startTime: startTimestamp, // Reset to original start for mints
      endTime: endTimestamp,
      maxRecords: MAX_RECORDS_PER_QUERY,
    };
    const data: any = await client.request(mintQuery, mintVariables);
    if (data.mints && data.mints.length > 0) {
      allMints = data.mints.map((m: any) => ({
        id: m.id,
        timestamp: m.timestamp,
        owner: m.owner,
        sender: m.sender,
        origin: m.origin,
        amount: ethers.BigNumber.from(m.amount).toString(),
        amount0: ethers.BigNumber.from(m.amount0).toString(),
        amount1: ethers.BigNumber.from(m.amount1).toString(),
        tickLower: ethers.BigNumber.from(m.tickLower).toString(),
        tickUpper: ethers.BigNumber.from(m.tickUpper).toString(),
      }));
      console.log(`Fetched ${allMints.length} mints.`);
    } else {
        console.log("No mints found in the specified range.");
    }
  } catch (error) {
    console.error("Error fetching mints from The Graph:", error);
  }

  // Fetch Burns (first batch only for this example)
  console.log("\nFetching Burns (first batch)...");
  const burnQuery = gql`
    query getBurns($poolAddress: String!, $startTime: Int!, $endTime: Int!, $maxRecords: Int!) {
      burns(
        first: $maxRecords
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
        amount
        amount0
        amount1
        tickLower
        tickUpper
      }
    }
  `;
  try {
    const burnVariables = {
      poolAddress: poolAddress.toLowerCase(),
      startTime: startTimestamp, // Reset to original start for burns
      endTime: endTimestamp,
      maxRecords: MAX_RECORDS_PER_QUERY,
    };
    const data: any = await client.request(burnQuery, burnVariables);
    if (data.burns && data.burns.length > 0) {
      allBurns = data.burns.map((b: any) => ({
        id: b.id,
        timestamp: b.timestamp,
        owner: b.owner,
        origin: b.origin,
        amount: ethers.BigNumber.from(b.amount).toString(),
        amount0: ethers.BigNumber.from(b.amount0).toString(),
        amount1: ethers.BigNumber.from(b.amount1).toString(),
        tickLower: ethers.BigNumber.from(b.tickLower).toString(),
        tickUpper: ethers.BigNumber.from(b.tickUpper).toString(),
      }));
      console.log(`Fetched ${allBurns.length} burns.`);
    } else {
        console.log("No burns found in the specified range.");
    }
  } catch (error) {
    console.error("Error fetching burns from The Graph:", error);
  }
  
  return { swaps: allSwaps, mints: allMints, burns: allBurns };
}

async function writeSwapsToCsv(swaps: SwapData[], filePath: string) {
  if (swaps.length === 0) {
    console.log("No swap data to write to CSV.");
    return;
  }
  const csvWriter = createObjectCsvWriter({
    path: filePath,
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
    ],
  });
  try {
    await csvWriter.writeRecords(swaps);
    console.log(`Swaps data successfully written to ${filePath}`);
  } catch (error) {
    console.error(`Error writing swaps to CSV ${filePath}:`, error);
  }
}

async function writeMintsToCsv(mints: MintData[], filePath: string) {
  if (mints.length === 0) {
    console.log("No mint data to write to CSV.");
    return;
  }
  const csvWriter = createObjectCsvWriter({
    path: filePath,
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
    ],
  });
  try {
    await csvWriter.writeRecords(mints);
    console.log(`Mints data successfully written to ${filePath}`);
  } catch (error) {
    console.error(`Error writing mints to CSV ${filePath}:`, error);
  }
}

async function writeBurnsToCsv(burns: BurnData[], filePath: string) {
  if (burns.length === 0) {
    console.log("No burn data to write to CSV.");
    return;
  }
  const csvWriter = createObjectCsvWriter({
    path: filePath,
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
    ],
  });
  try {
    await csvWriter.writeRecords(burns);
    console.log(`Burns data successfully written to ${filePath}`);
  } catch (error) {
    console.error(`Error writing burns to CSV ${filePath}:`, error);
  }
}


async function main() {
  // if (INFURA_URL === 'YOUR_RPC_URL_HERE') {
  //   console.warn("Please replace 'YOUR_RPC_URL_HERE' with your actual RPC URL in src/fetch_data.ts");
  //   // For demonstration, we'll proceed, but SDK calls will likely fail.
  // }
  const provider = new ethers.providers.JsonRpcProvider(INFURA_URL);
  const graphQLClient = new GraphQLClient(THE_GRAPH_URL);

  // Call the function to get current pool data via SDK (optional, can be commented out if not needed)
  // await getPoolDataFromSDK(POOL_ADDRESS, provider);

  // Define time range for historical data
  // Example: A 1-hour window from 2 days ago to 1 day and 23 hours ago
  const currentTimestamp = Math.floor(Date.now() / 1000);
  // const endTimestamp = currentTimestamp - (24 * 60 * 60); // 1 day ago
  // const startTimestamp = endTimestamp - (1 * 60 * 60); // 1 hour before endTimestamp
  
  // More recent 1hr window for testing (ensure data exists for the pool in this window)
  // For USDC/WETH 0.05% (0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640), let's try a recent period
  // For example, a 1-hour window starting 2 hours ago
  const startTimestamp = currentTimestamp - (2 * 60 * 60); // 2 hours ago
  const endTimestamp = currentTimestamp - (1 * 60 * 60); // 1 hour ago


  const { swaps, mints, burns } = await fetchHistoricalDataWithTheGraph(
    POOL_ADDRESS,
    graphQLClient,
    startTimestamp,
    endTimestamp
  );

  // Write data to CSV files
  await writeSwapsToCsv(swaps, 'swaps.csv');
  await writeMintsToCsv(mints, 'mints.csv');
  await writeBurnsToCsv(burns, 'burns.csv');

  console.log("\n--- Data fetching and writing complete ---");
}

main().catch((error) => {
  console.error("Error in main execution:", error);
  process.exitCode = 1;
});
