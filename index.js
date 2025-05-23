// pollAndChain.js
const { Firestore }       = require('@google-cloud/firestore');
const { JobsClient }      = require('@google-cloud/run').v2;
const axios               = require('axios');

const firestore           = new Firestore({
  projectId: 'evergreen-45696013',
  databaseId: 'imports'
});
const runClient           = new JobsClient();

const PROJECT_ID          = 'evergreen-45696013';
const REGION              = 'us-central1';
const JOB_NAME            = 'importer-job';
const RUNS_COLLECTION     = 'imports';
const HUBSPOT_API_KEY     = process.env.HUBSPOT_API_KEY;
const HUBSPOT_STATUS_URL  = 'https://api.hubapi.com/crm/v3/imports'; 

// Batch definitions
const BATCH_FILES = {
  1: ['test0'],
  2: ['test1'],
  3: ['test2']
};

console.log('Deploying')

async function main() {

  console.log('Initializing')
  
  const runsSnap = await firestore.collection(RUNS_COLLECTION)
    .where('batches', '!=', null)
    .get();

  for (const doc of runsSnap.docs) {
    const data     = doc.data();
    const runId    = doc.id;
    const batchNum = data.currentBatch;
    const batchKey = `batch${batchNum}`;
    const batch    = data.batches?.[batchKey];
    if (!batch || batch.status !== 'in_progress' || !batch.importId) continue;

    // 1) Poll HubSpot
    let resp;
    try {
      resp = await axios.get(`${HUBSPOT_STATUS_URL}/${batch.importId}`, {
        headers: { Authorization: `Bearer ${HUBSPOT_API_KEY}` }
      });
    } catch (e) {
      console.error(`Error polling import ${batch.importId}:`, e);
      continue;
    }

    const state = resp.data.state;  // "IN_PROGRESS" | "DONE" | "FAILED"
    if (state === 'DONE' || state === 'FAILED') {
      // 2) Update Firestore: mark batch complete AND bump currentBatch
      const nextBatch = batchNum + 1;
      console.log(`Run ${runId} batch${batchNum} is ${state}; bumping to batch${nextBatch}`);

      await firestore.collection(RUNS_COLLECTION).doc(runId).update({
        // mark this batch status
        [`batches.${batchKey}.status`]: state === 'DONE' ? 'complete' : 'failed',
        // atomically increment currentBatch
        currentBatch: FieldValue.increment(1),
        // record timestamp
        [`batches.${batchKey}.updatedAt`]: FieldValue.serverTimestamp()
      });

      // 3) If we still have another batch to run, launch the importer Job
      if (data.batches[`batch${nextBatch}`]) {
        const jobPath = `projects/${PROJECT_ID}/locations/${REGION}/jobs/${IMPORTER_JOB}`;
        console.log(`→ launching importer job for run ${runId}, batch ${nextBatch}`);
        await runClient.runJob({ name: jobPath });
      } else {
        console.log(`✔ All batches complete for run ${runId}`);
      }
    }
  }
};

main()
