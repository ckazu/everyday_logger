GCP_PROJECT_ID = process.env.GCP_PROJECT_ID;
GCP_BQ_DATASET = process.env.GCP_BQ_DATASET || 'log';

const moment = require('moment');
const { BigQuery } = require('@google-cloud/bigquery');
const bigquery = new BigQuery({
  projectId: GCP_PROJECT_ID
});

exports.everydayLogger = async (req, res) => {
  const body = req.body;
  console.log(body);

  var time, topic, label, value, payload;

  try {
    if(body == null) { throw new Error('EmptyBody'); }

    if(body.time == null) { time = moment().format(); }
    if(moment(body.time).isValid()) {
      time = moment(body.time).format();
    } else {
      throw new Error('InvalidTimeFormat');
    }

    topic = body.topic;
    if(topic == null) { throw new Error('EmptyTopic'); }

    label = body.label || null;
    value = body.value || null;
    payload = JSON.stringify(body.payload || {});
  } catch (e) {
    console.error('validation: ', e);
    res.status(500).end(`error: ${e}`);
    return;
  }

  const rows = [{ time: time, label: label, value: value, payload: payload }];
  console.log(rows);
  await routine(GCP_PROJECT_ID, GCP_BQ_DATASET, topic, rows);

  res.send('ok');
};

routine = async (project, dataset, topic, rows, wait=0) => {
  await sleep(wait);
  return await insertRows(dataset, topic, rows)
    .catch (err => {
      if(err.message == `Not found: Dataset ${project}:${dataset}`) {
        createDataset(dataset, topic)
          .then(_ => {
            createTable(dataset, topic);
          }).then(_ => {
            routine(project, dataset, topic, rows, 3000);
          }).catch(err => {
            routine(project, dataset, topic, rows, 3000);
          });
      } else if(err.message == `Not found: Table ${project}:${dataset}.${topic}`) {
        createTable(dataset, topic)
          .then(_ => {
            routine(project, dataset, topic, rows, 3000);
          }).catch(err => {
            routine(project, dataset, topic, rows, 3000);
          });
      } else {
        console.error(err);
        throw err;
        return;
      }
    });
};

insertRows = async (dataset, topic, rows) => {
  await bigquery.dataset(dataset).table(topic).insert(rows)
    .then(_ => {
      console.log(`Inserted ${topic}`);
    }).catch (err => {
      throw err;
    });
};

createDataset = async (dataset, topic) => {
  const options = {  };
  await bigquery.createDataset(dataset, options)
    .then(([res]) => {
      console.log(`Dataset ${res.id} created.`);
    }).catch(err => {
      throw err;
    });
};

createTable = async (dataset, table) => {
  const options = {
    schema: [{ name: 'time',    type: 'TIMESTAMP', mode: 'REQUIRED' },
             { name: 'label',   type: 'STRING',    mode: 'NULLABLE' },
             { name: 'value',   type: 'NUMERIC',   mode: 'NULLABLE' },
             { name: 'payload', type: 'STRING',    mode: 'NULLABLE' }]
  };
  await bigquery.dataset(dataset).createTable(table, options)
    .then(([res]) => {
      console.log(`Table ${res.id} created.`);
    }).catch(err => {
      throw err;
    });
};

sleep = async (time) => {
  return new Promise((resolve, reject) => {
    setTimeout(() => {
      resolve();
    }, time);
  });
};
