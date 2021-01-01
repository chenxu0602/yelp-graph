
CALL apoc.periodic.iterate("
CALL apoc.load.csv('file:///dataset/csv/business.csv', {
  mapping: {
    business_id: {type: 'str'},
    name: {type: 'str'},
    address: {type: 'str'},
    city: {type: 'str'},
    state: {type: 'str'},
    postal_code:{type: 'str'},
    latitude: {type: 'float'},
    longitude: {type: 'float'},
    stars: {type: 'float'}
  }
}) YIELD map as row return row
","
MERGE (b:Business {id: row.business_id})
SET b.name = row.name,
    b.address = row.address,
    b.city = row.city,
    b.state = row.state,
    b.postal_code = row.postal_code,
    b.latitude = row.latitude,
    b.longitude = row.longitude,
    b.stars = row.stars
", {batchSize:10000, iterateList:true, parallel:true});
