// poller.js
const { Firestore } = require('@google-cloud/firestore');
const axios       = require('axios');

const firestore = new Firestore();

const RUNS_COLLECTION  = 'imports';
const HUBSPOT_API_KEY  = process.env.HUBSPOT_API_KEY;
const HUBSPOT_STATUS_URL = 'https://api.hubapi.com/crm/v3/imports'; // append /{id}

// Exposed as a Pub/Sub–triggered function (via Cloud Scheduler)
exports.pollImportStatus = async (pubsubEvent, context) => {
  try {
    // 1) Load all runs where current batch is still in_progress
    const runsSnap = await firestore.collection(RUNS_COLLECTION)
      .where('batches', '!=', null)     // document must have batches
      .get();

    const now = new Date().toISOString();
    for (const doc of runsSnap.docs) {
      const data = doc.data();
      const runId = doc.id;
      const batchNum = data.currentBatch;
      const batchKey = `batch${batchNum}`;
      const batch = data.batches && data.batches[batchKey];
      if (!batch || batch.status !== 'in_progress' || !batch.importId) {
        continue;
      }

      const importId = batch.importId;
      // 2) Query HubSpot for status
      const url = `${HUBSPOT_STATUS_URL}/${importId}`;
      let resp;
      try {
        resp = await axios.get(url, {
          headers: { Authorization: `Bearer ${HUBSPOT_API_KEY}` }
        });
      } catch (e) {
        console.error(`Run ${runId} batch ${batchKey}: error fetching status`, e);
        continue;
      }

      const state = resp.data.state; // e.g. "IN_PROGRESS", "DONE", "FAILED"
      console.log(`${now} - Run ${runId} batch ${batchKey} import ${importId} is ${state}`);

      // 3) Update Firestore if complete or failed
      if (state === 'DONE' || state === 'FAILED') {
        await firestore.collection(RUNS_COLLECTION).doc(runId).update({
          [`batches.${batchKey}.status`]: state === 'DONE' ? 'complete' : 'failed',
          [`batches.${batchKey}.updatedAt`]: Firestore.FieldValue.serverTimestamp()
        });
        console.log(`  → Marked ${batchKey} as ${state === 'DONE' ? 'complete' : 'failed'}`);
      }
    }
  } catch (err) {
    console.error('pollImportStatus error:', err);
  }
};
