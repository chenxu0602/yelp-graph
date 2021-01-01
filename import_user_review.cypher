
CALL apoc.periodic.iterate("
CALL apoc.load.csv('file:///dataset/csv/user_review.csv', {
  mapping: {
    user_id: {type: 'str'},
    review_id: {type: 'str'}
  }
}) YIELD map as row return row
","
MATCH (u:User {id: row.user_id})
MATCH (r:Review {id: row.review_id})
MERGE (u)-[:WROTE]->(r)
", {batchSize:100, iterateList:true, parallel:true});

