
CALL apoc.periodic.iterate("
CALL apoc.load.csv('file:///dataset/csv/review.csv', {
  mapping: {
    review_id: {type: 'str'},
    stars: {type: 'float'},
    date: {type: 'date'}
  }
}) YIELD map as row return row
","
MERGE (r:Review {id: row.review_id})
SET r.stars = row.stars,
    r.date = row.date
", {batchSize:100, iterateList:true, parallel:true});

