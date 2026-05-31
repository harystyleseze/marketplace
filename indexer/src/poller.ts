import { rpc } from '@stellar/stellar-sdk';
import prisma from './db';
import { parseMarketplaceEvent } from './parser';
import dotenv from 'dotenv';

dotenv.config();

const RPC_URL = process.env.STELLAR_RPC_URL || 'https://soroban-testnet.stellar.org';
const CONTRACT_ID = process.env.MARKETPLACE_CONTRACT_ID || '';
const POLL_INTERVAL = parseInt(process.env.POLL_INTERVAL_MS || '5000');

const server = new rpc.Server(RPC_URL);

export async function startPolling() {
  console.log(`Starting indexer poller for contract: ${CONTRACT_ID}`);

  while (true) {
    try {
      // 1. Get last indexed ledger
      let syncState = await prisma.syncState.findUnique({ where: { id: 1 } });
      if (!syncState) {
        syncState = await prisma.syncState.create({ data: { id: 1, lastLedger: 0 } });
      }

      // 2. Fetch network state to know current ledger
      // const networkDetails = await server.getNetwork();
      
      // 3. Get events from lastLedger + 1
      const startLedger = syncState.lastLedger + 1;
      
      const response = await server.getEvents({
        startLedger: startLedger,
        filters: [
          {
            type: 'contract',
            contractIds: [CONTRACT_ID],
          },
        ],
      });

      if (response.events && response.events.length > 0) {
        console.log(`Found ${response.events.length} new events since ledger ${syncState.lastLedger}`);
        
        let maxLedger = syncState.lastLedger;

        for (const event of response.events) {
          // Topics in v14 are ScVal, need to convert to strings (symbol or other)
          const topicStrings = event.topic.map(t => {
            if (typeof t === 'string') return t; // Already a string/base64
            return t.toXDR('base64'); // If it's an ScVal object
          });
          
          const decoded = parseMarketplaceEvent(
            topicStrings, 
            typeof event.value === 'string' ? event.value : event.value.toXDR('base64'), 
            event.ledger
          );
          if (decoded) {
            await processEvent(decoded);
          }
          if (event.ledger > maxLedger) maxLedger = event.ledger;
        }

        // Update sync state
        await prisma.syncState.update({
          where: { id: 1 },
          data: { lastLedger: maxLedger },
        });
      }

    } catch (error) {
      console.error('Error in polling loop:', error);
    }

    await new Promise((resolve) => setTimeout(resolve, POLL_INTERVAL));
  }
}

async function processEvent(event: any) {
  const { eventType, listingId, actor, ledgerSequence, data } = event;

  const eventPayload = {
    listingId,
    eventType,
    actor,
    ledgerSequence,
    data,
  };

  if (!listingId) {
    await prisma.marketplaceEvent.create({ data: eventPayload });
    return;
  }

  try {
    await prisma.$transaction(async (tx) => {
      await tx.marketplaceEvent.create({ data: eventPayload });

      switch (eventType) {
        case 'LISTING_CREATED':
          await tx.listing.upsert({
            where: { listingId },
            create: {
              listingId,
              artist: data.artist,
              owner: null,
              price: data.price,
              currency: data.currency,
              metadataCid: data.metadata_cid,
              token: data.token || '',
              status: 'Active',
              royaltyBps: data.royalty_bps || 0,
              createdAtLedger: ledgerSequence,
              updatedAtLedger: ledgerSequence,
            },
            update: {
              artist: data.artist,
              price: data.price,
              metadataCid: data.metadata_cid,
              status: 'Active',
              updatedAtLedger: ledgerSequence,
            },
          });
          break;

        case 'LISTING_UPDATED':
          await tx.listing.update({
            where: { listingId },
            data: {
              price: data.new_price,
              metadataCid: data.metadata_cid,
              updatedAtLedger: ledgerSequence,
            },
          });
          break;

        case 'ARTWORK_SOLD':
          await tx.listing.update({
            where: { listingId },
            data: {
              status: 'Sold',
              owner: data.buyer,
              updatedAtLedger: ledgerSequence,
            },
          });
          break;

        case 'OFFER_ACCEPTED':
          await tx.listing.update({
            where: { listingId },
            data: {
              status: 'Sold',
              owner: data.offerer,
              price: data.amount,
              updatedAtLedger: ledgerSequence,
            },
          });
          break;

        case 'LISTING_CANCELLED':
          await tx.listing.update({
            where: { listingId },
            data: {
              status: 'Cancelled',
              updatedAtLedger: ledgerSequence,
            },
          });
          break;

        case 'AUCTION_CREATED':
          await tx.listing.update({
            where: { listingId },
            data: {
              status: 'Auction',
              updatedAtLedger: ledgerSequence,
            },
          });
          break;

        default:
          break;
      }
    });
  } catch (error) {
    console.error(`Failed to process event ${eventType} for listing ${listingId}:`, error);
    throw error;
  }
}
