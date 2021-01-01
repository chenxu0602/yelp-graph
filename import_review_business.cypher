
CALL apoc.periodic.iterate("
CALL apoc.load.csv('file:///dataset/csv/review_business.csv', {
  mapping: {
    review_id: {type: 'str'},
    business_id: {type: 'str'}
  }
}) YIELD map as row return row
","
MATCH (r:Review {id: row.review_id})
MATCH (u:Business {id: row.business_id})
MERGE (r)-[:REVIEWS]->(u)
", {batchSize:100, iterateList:true, parallel:true});

