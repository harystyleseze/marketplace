"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.revertLedgers = revertLedgers;
exports.startPolling = startPolling;
exports.processEvent = processEvent;
const stellar_sdk_1 = require("@stellar/stellar-sdk");
const db_js_1 = __importDefault(require("./db.js"));
const parser_js_1 = require("./parser.js");
const dotenv_1 = __importDefault(require("dotenv"));
const metrics_js_1 = require("./metrics.js");
dotenv_1.default.config();
const RPC_URL = process.env.STELLAR_RPC_URL || 'https://soroban-testnet.stellar.org';
const CONTRACT_ID = process.env.MARKETPLACE_CONTRACT_ID || '';
const LAUNCHPAD_CONTRACT_ID = process.env.LAUNCHPAD_CONTRACT_ID || '';
const POLL_INTERVAL = parseInt(process.env.POLL_INTERVAL_MS || '5000');
const server = new stellar_sdk_1.rpc.Server(RPC_URL);
async function revertLedgers(toLedger) {
    console.log(`Reverting database to ledger ${toLedger}...`);
    // 1. Get the new hash for the target ledger from the network
    let newHash = null;
    if (toLedger > 0) {
        try {
            const ledgersRes = await server.getLedgers({
                startLedger: toLedger,
                pagination: { limit: 1 }
            });
            if (ledgersRes.ledgers && ledgersRes.ledgers.length > 0) {
                newHash = ledgersRes.ledgers[0].hash;
            }
        }
        catch (err) {
            console.error(`Failed to fetch hash for ledger ${toLedger} during revert:`, err);
        }
    }
    // 2. Perform DB operations inside a transaction to ensure safety
    await db_js_1.default.$transaction(async (tx) => {
        // A. Revert/update listings that were updated in the reverted ledgers
        const listingIdsToRevert = await tx.listing.findMany({
            where: {
                updatedAtLedger: { gt: toLedger }
            },
            select: {
                listingId: true,
                createdAtLedger: true
            }
        });
        for (const listing of listingIdsToRevert) {
            if (listing.createdAtLedger > toLedger) {
                // If created in a reverted ledger, delete it
                await tx.listing.delete({
                    where: { listingId: listing.listingId }
                });
            }
            else {
                // Fetch all events for this listing up to toLedger, ordered by sequence and ID
                const events = await tx.marketplaceEvent.findMany({
                    where: {
                        listingId: listing.listingId,
                        ledgerSequence: { lte: toLedger }
                    },
                    orderBy: [
                        { ledgerSequence: 'asc' },
                        { id: 'asc' }
                    ]
                });
                // Replay events to reconstruct the listing state as of toLedger
                let listingState = null;
                for (const event of events) {
                    const { eventType, ledgerSequence, data } = event;
                    const parsedData = typeof data === 'string' ? JSON.parse(data) : data;
                    if (eventType === 'LISTING_CREATED') {
                        listingState = {
                            artist: parsedData.artist,
                            owner: null,
                            price: parsedData.price,
                            currency: parsedData.currency,
                            metadataCid: parsedData.metadata_cid,
                            token: parsedData.token || '',
                            status: 'Active',
                            royaltyBps: parsedData.royalty_bps || 0,
                            createdAtLedger: ledgerSequence,
                            updatedAtLedger: ledgerSequence,
                        };
                    }
                    else if (listingState) {
                        if (eventType === 'LISTING_UPDATED') {
                            listingState.price = parsedData.new_price;
                            listingState.metadataCid = parsedData.metadata_cid;
                            listingState.updatedAtLedger = ledgerSequence;
                        }
                        else if (eventType === 'ARTWORK_SOLD') {
                            listingState.status = 'Sold';
                            listingState.owner = parsedData.buyer;
                            listingState.updatedAtLedger = ledgerSequence;
                        }
                        else if (eventType === 'LISTING_CANCELLED') {
                            listingState.status = 'Cancelled';
                            listingState.updatedAtLedger = ledgerSequence;
                        }
                        else if (eventType === 'AUCTION_CREATED') {
                            listingState.status = 'Auction';
                            listingState.updatedAtLedger = ledgerSequence;
                        }
                    }
                }
                if (listingState) {
                    await tx.listing.update({
                        where: { listingId: listing.listingId },
                        data: {
                            status: listingState.status,
                            owner: listingState.owner,
                            price: listingState.price,
                            metadataCid: listingState.metadataCid,
                            updatedAtLedger: listingState.updatedAtLedger,
                        }
                    });
                }
                else {
                    // If no events exist as of toLedger, delete it
                    await tx.listing.delete({
                        where: { listingId: listing.listingId }
                    });
                }
            }
        }
        // B. Revert collections that were deployed after toLedger
        await tx.collection.deleteMany({
            where: {
                deployedAtLedger: { gt: toLedger }
            }
        });
        // C. Delete events that occurred after toLedger
        await tx.marketplaceEvent.deleteMany({
            where: {
                ledgerSequence: { gt: toLedger }
            }
        });
        // D. Update SyncState to the reverted ledger and new hash
        await tx.syncState.update({
            where: { id: 1 },
            data: {
                lastLedger: toLedger,
                ledgerHash: newHash
            }
        });
    });
    console.log(`Successfully reverted database to ledger ${toLedger}`);
}
async function startPolling() {
    console.log(`Starting indexer poller for contract: ${CONTRACT_ID}`);
    while (true) {
        try {
            // 1. Get last indexed ledger
            let syncState = await db_js_1.default.syncState.findUnique({ where: { id: 1 } });
            if (!syncState) {
                syncState = await db_js_1.default.syncState.create({
                    data: { id: 1, lastLedger: 0, ledgerHash: null }
                });
            }
            // 2. Validate hash continuity on every poll
            if (syncState.lastLedger > 0 && syncState.ledgerHash) {
                try {
                    const ledgersRes = await server.getLedgers({
                        startLedger: syncState.lastLedger,
                        pagination: { limit: 1 }
                    });
                    if (ledgersRes.ledgers && ledgersRes.ledgers.length > 0) {
                        const networkLedger = ledgersRes.ledgers[0];
                        if (networkLedger.hash !== syncState.ledgerHash) {
                            console.warn(`Chain re-org detected at ledger ${syncState.lastLedger}! DB hash: ${syncState.ledgerHash}, Network hash: ${networkLedger.hash}`);
                            const toLedger = Math.max(0, syncState.lastLedger - 1);
                            await revertLedgers(toLedger);
                            continue; // Restart the loop immediately with the reverted state
                        }
                    }
                }
                catch (err) {
                    console.error(`Error validating ledger hash continuity at ledger ${syncState.lastLedger}:`, err);
                }
            }
            // 3. Get events from lastLedger + 1
            const startLedger = syncState.lastLedger + 1;
            const response = await server.getEvents({
                startLedger: startLedger,
                filters: [
                    {
                        type: 'contract',
                        contractIds: [CONTRACT_ID, LAUNCHPAD_CONTRACT_ID].filter(Boolean),
                    },
                ],
            });
            // 4. Update metrics gauges
            const networkLatest = response.latestLedger || syncState.lastLedger;
            // Update gauges
            metrics_js_1.latestLedgerProcessedGauge.set(syncState.lastLedger);
            metrics_js_1.networkLatestLedgerGauge.set(networkLatest);
            metrics_js_1.syncLatencyGauge.set(Math.max(0, networkLatest - syncState.lastLedger));
            if (response.events && response.events.length > 0) {
                console.log(`Found ${response.events.length} new events since ledger ${syncState.lastLedger}`);
                let maxLedger = syncState.lastLedger;
                for (const event of response.events) {
                    // Topics in v14 are ScVal, need to convert to strings (symbol or other)
                    const topicStrings = event.topic.map(t => {
                        if (typeof t === 'string')
                            return t; // Already a string/base64
                        return t.toXDR('base64'); // If it's an ScVal object
                    });
                    const decoded = (0, parser_js_1.parseMarketplaceEvent)(topicStrings, typeof event.value === 'string' ? event.value : event.value.toXDR('base64'), event.ledger);
                    if (decoded) {
                        await processEvent(decoded);
                    }
                    if (event.ledger > maxLedger)
                        maxLedger = event.ledger;
                }
                // We successfully indexed events up to maxLedger.
                // Fetch the hash for this ledger to maintain the hash continuity chain.
                let newHash = null;
                try {
                    const ledgersRes = await server.getLedgers({
                        startLedger: maxLedger,
                        pagination: { limit: 1 }
                    });
                    if (ledgersRes.ledgers && ledgersRes.ledgers.length > 0) {
                        newHash = ledgersRes.ledgers[0].hash;
                    }
                }
                catch (err) {
                    console.error(`Failed to fetch hash for ledger ${maxLedger}:`, err);
                }
                // Update sync state
                const updatedState = await db_js_1.default.syncState.update({
                    where: { id: 1 },
                    data: {
                        lastLedger: maxLedger,
                        ledgerHash: newHash,
                    },
                });
                metrics_js_1.latestLedgerProcessedGauge.set(updatedState.lastLedger);
                metrics_js_1.syncLatencyGauge.set(Math.max(0, networkLatest - updatedState.lastLedger));
            }
            else if (response.latestLedger && response.latestLedger > syncState.lastLedger) {
                // If there are no events but the network has advanced, we can catch up the syncState
                // so we don't scan empty ranges repeatedly. Fetch the hash for the latest ledger.
                let newHash = null;
                try {
                    const ledgersRes = await server.getLedgers({
                        startLedger: response.latestLedger,
                        pagination: { limit: 1 }
                    });
                    if (ledgersRes.ledgers && ledgersRes.ledgers.length > 0) {
                        newHash = ledgersRes.ledgers[0].hash;
                    }
                }
                catch (err) {
                    console.error(`Failed to fetch hash for latest network ledger ${response.latestLedger}:`, err);
                }
                const updatedState = await db_js_1.default.syncState.update({
                    where: { id: 1 },
                    data: {
                        lastLedger: response.latestLedger,
                        ledgerHash: newHash,
                    },
                });
                metrics_js_1.latestLedgerProcessedGauge.set(updatedState.lastLedger);
                metrics_js_1.syncLatencyGauge.set(Math.max(0, networkLatest - updatedState.lastLedger));
            }
        }
        catch (error) {
            console.error('Error in polling loop:', error);
        }
        await new Promise((resolve) => setTimeout(resolve, POLL_INTERVAL));
    }
}
async function processEvent(event) {
    const { eventType, listingId, actor, ledgerSequence, data } = event;
    // 1. Log to MarketplaceEvent history
    await db_js_1.default.marketplaceEvent.create({
        data: {
            listingId,
            eventType,
            actor,
            ledgerSequence,
            data,
        },
    });
    // 2. Handle deploy events (no listingId — collection deployments)
    if (eventType === 'DEPLOY_NORMAL_721' || eventType === 'DEPLOY_NORMAL_1155' ||
        eventType === 'DEPLOY_LAZY_721' || eventType === 'DEPLOY_LAZY_1155') {
        const kindMap = {
            DEPLOY_NORMAL_721: 'normal_721',
            DEPLOY_NORMAL_1155: 'normal_1155',
            DEPLOY_LAZY_721: 'lazy_721',
            DEPLOY_LAZY_1155: 'lazy_1155',
        };
        const rawData = Array.isArray(data) ? data : [];
        const creatorAddr = rawData[0]?.toString() || actor;
        const contractAddr = rawData[1]?.toString() || '';
        if (contractAddr) {
            await db_js_1.default.collection.upsert({
                where: { contractAddress: contractAddr },
                create: {
                    contractAddress: contractAddr,
                    kind: kindMap[eventType],
                    creator: creatorAddr,
                    deployedAtLedger: ledgerSequence,
                },
                update: {
                    creator: creatorAddr,
                    deployedAtLedger: ledgerSequence,
                },
            });
        }
        return;
    }
    // 3. Update Listing state based on event type
    if (!listingId)
        return;
    switch (eventType) {
        case 'LISTING_CREATED':
            await db_js_1.default.listing.upsert({
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
                }
            });
            break;
        case 'LISTING_UPDATED':
            await db_js_1.default.listing.update({
                where: { listingId },
                data: {
                    price: data.new_price,
                    metadataCid: data.metadata_cid,
                    updatedAtLedger: ledgerSequence,
                },
            });
            break;
        case 'ARTWORK_SOLD':
            await db_js_1.default.listing.update({
                where: { listingId },
                data: {
                    status: 'Sold',
                    owner: data.buyer,
                    updatedAtLedger: ledgerSequence,
                },
            });
            break;
        case 'LISTING_CANCELLED':
            await db_js_1.default.listing.update({
                where: { listingId },
                data: {
                    status: 'Cancelled',
                    updatedAtLedger: ledgerSequence,
                },
            });
            break;
        // For Auctions and Offers, we might add more logic or separate tables if needed.
        // For now, we mainly update listing status if an auction starts.
        case 'AUCTION_CREATED':
            await db_js_1.default.listing.update({
                where: { listingId },
                data: {
                    status: 'Auction',
                    updatedAtLedger: ledgerSequence,
                }
            });
            break;
    }
}
