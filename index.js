const cors = require('cors')
const express = require('express')
const { Kafka } = require('kafkajs')

const kafka = new Kafka({
  clientId: 'my-app',
  brokers: ['my-kafka-0.my-kafka-headless.fermindra.svc.cluster.local:9092']
});

const producer = kafka.producer()

const app = express();
app.use(cors());
app.options('*', cors());

const port = 8080;

app.get('/', (req, res, next) => {
  res.send('comments api - fermindra');
});

const run = async (uId, oId, comment) => {

    await producer.connect()
//    await producer.send()
    await producer.send({
      topic: 'comments',
      messages: [ 
    { 
      'value': `{ "userId": "${uId}",  "objectId": "${oId}", "comment": "${comment}"}`
    } 
      ],
    })
   await producer.disconnect()
}

app.get('/comments', (req, res, next) => {
  const uId = req.query.userId;
  const oId = req.query.objectId;
  const comment = req.query.comment;
  res.send({'userId:': uId, 'objectID': oId,'comment' : comment} );
  run(uId, oId, comment).catch(e => console.error(`[example/producer] ${e.message}`, e))

});

app.listen(port,  () => 
	console.log('listening on port ' + port
));
