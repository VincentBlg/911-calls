const csv = require('csv-parser');
const fs = require('fs');
const { Client } = require('@elastic/elasticsearch');

async function run() {
    const client = new Client({ node: 'http://localhost:9200'});

// Drop index if exists
await client.indices.delete({
    index: 'calls',
    ignore_unavailable: true
    });


// Création de l'indice
client.indices.create({ index: 'calls', body: {mappings: {properties:{location:{type :"geo_point" }, date : {type : "date"}}}}}, (err, resp) => {
    if (err) console.log(err.message);
});

let calls = [];
fs
.createReadStream('../911.csv')
.pipe(csv())
// Pour chaque ligne on créé un document JSON pour l'acteur correspondant
.on('data', data => {
    calls.push({
        lat: data.lat,
        lng: data.lng,
		location : data.lat + ", " + data.lng,
		date: data.timeStamp.substr(0,10),
        desc: data.desc,
        zip: data.zip,
        title1: data.title.split(":")[0],
        title2: data.title.split(":")[1],
        timeStamp: data.timeStamp.substr(5,2) + "/"+ data.timeStamp.substr(0,4),
        twp: data.twp,
        addr: data.addr,
        e: data.e
        });
        })
// A la fin on créé l'ensemble des appels dans ElasticSearch
    .on('end', () => {
        client.bulk(createBulkInsertQuery(calls), (err, resp) => {
            if (err) console.log(err.message);
            else console.log(`Inserted ${resp.body.items.length} calls`);
            client.close();
            });
            });
}

run().catch(console.log);

// Fonction utilitaire permettant de formatter les données pour l'insertion "bulk" dans elastic
function createBulkInsertQuery(calls) {
    const body = calls.reduce((acc, call) => {
        const { lat, lng, location, date, desc, zip, title1, title2, timeStamp, twp, addr, e } = call;
        acc.push({ index: { _index: 'calls', _type: '_doc'}})
        acc.push({ lat, lng, location, date, desc, zip, title1, title2, timeStamp, twp, addr, e })
        return acc
        }, []);
        return { body };
}
