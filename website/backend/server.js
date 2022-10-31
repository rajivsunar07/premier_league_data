const cassandra = require('cassandra-driver');
const express = require('express')
const app = express()

var PlainTextAuthProvider = cassandra.auth.PlainTextAuthProvider;
const client = new cassandra.Client({
  contactPoints: ['127.0.0.1:9042'],
  localDataCenter: 'datacenter1',
  authProvider: new PlainTextAuthProvider('cassandra', 'cassandra')
});



app.use((req, res, next) => {
  res.header("Access-Control-Allow-Origin", "*");
  res.header(
    "Access-Control-Allow-Headers",
    "Origin, X-Requested-With, Content-Type, Accept, Authorization"
  );
  if (req.method === "OPTIONS") {
    res.header("Access-Control-Allow-Methods", "PUT, POST, PATCH, DELETE, GET");
    return res.status(200).json({});
  }
  next();
});

app.get('/upcoming_fixtures', (req, res) => {
  const query = "SELECT * FROM premier.fixtures_by_gameweek where finished=False order by started, kickoff_time limit 10";
  client.execute(query)
  .then(result => {
    console.log(result.rows)
    res.json(result.rows)
  })

})

app.listen(5000, () => console.log('server running'))