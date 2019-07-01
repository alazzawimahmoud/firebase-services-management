import * as admin from 'firebase-admin';

export function deleteCollection(db: FirebaseFirestore.Firestore, collectionPath: string, batchSize: number) {
  let collectionRef = db.collection(collectionPath);
  let query = collectionRef.orderBy('timestamp').limit(batchSize);

  return new Promise((resolve, reject) => {
    deleteQueryBatch(db, query, batchSize, resolve, reject);
  });
}

let counter = 0;
export function deleteQueryBatch(
  db: FirebaseFirestore.Firestore,
  query: FirebaseFirestore.Query,
  batchSize: number,
  resolve: (value?: {} | PromiseLike<{}> | undefined) => void,
  reject: (reason?: any) => void
) {
  console.log(`[deleteQueryBatch] started `)
  query.get()
    .then((snapshot) => {

      // When there are no documents left, we are done
      if (snapshot.size == 0) {
        return 0;
      }

      // Delete documents in a batch
      let batch = db.batch();
      snapshot.docs.forEach((doc) => {
        batch.delete(doc.ref);
      });

      return batch.commit().then(() => {
        counter = batchSize + counter;
        return snapshot.size;
      });
    }).then((numDeleted) => {
      if (numDeleted === 0) {
        console.log(`[deleteQueryBatch] done deleting ${counter + batchSize}`)
        resolve();
        return;
      }
      console.log(`[deleteQueryBatch] ${counter} deleted`)
      // Recurse on the next process tick, to avoid
      // exploding the stack.
      process.nextTick(() => {
        deleteQueryBatch(db, query, batchSize, resolve, reject);
      });
    })
    .catch(reject);
}