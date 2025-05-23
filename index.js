// pollAndChain.js
const { Firestore }       = require('@google-cloud/firestore');
const { JobsClient }  = require('@google-cloud/run');
const axios               = require('axios');

const firestore           = new Firestore();
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

exports.pollAndChain = async (pubsubEvent, context) => {
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
    const importId = batch.importId;
    let resp;
    try {
      resp = await axios.get(`${HUBSPOT_STATUS_URL}/${importId}`, {
        headers: { Authorization: `Bearer ${HUBSPOT_API_KEY}` }
      });
    } catch (e) {
      console.error(`Error polling import ${importId}:`, e);
      continue;
    }
    const state = resp.data.state; // e.g. "IN_PROGRESS", "DONE", "FAILED"

    if (state === 'DONE' || state === 'FAILED') {
      const newStatus = state === 'DONE' ? 'complete' : 'failed';
      console.log(`Run ${runId} batch${batchNum} is ${newStatus}`);

      // 2) Update Firestore
      await firestore.collection(RUNS_COLLECTION).doc(runId).update({
        [`batches.${batchKey}.status`]: newStatus,
        [`batches.${batchKey}.updatedAt`]: Firestore.FieldValue.serverTimestamp()
      });

      // 3) If done, chain next batch
      if (newStatus === 'complete') {
        const nextBatch = batchNum + 1;
        if (BATCH_FILES[nextBatch]) {
          console.log(`→ Launching batch${nextBatch} for run ${runId}`);
          const parent  = `projects/${PROJECT_ID}/locations/${REGION}`;
          const jobPath = `${parent}/jobs/${JOB_NAME}`;

          await runClient.runJob({
            name: jobPath,
            execution: {
              template: {
                containers: [{
                  image: `gcr.io/${PROJECT_ID}/${JOB_NAME}:latest`,
                  args: [`--runId=${runId}`, `--batchNum=${nextBatch}`]
                }]
              }
            }
          });
        } else {
          console.log(`✔ All batches complete for run ${runId}`);
        }
      }
    }
  }
};
