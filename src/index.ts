import * as admin from 'firebase-admin';
import getCollection from './GetCollection';
interface IDeleteCollectionProps {
  db: FirebaseFirestore.Firestore, collectionPath?: string, batchSize?: number, query?: FirebaseFirestore.Query
}
export function deleteCollection({ db, collectionPath, batchSize = 10, query }: IDeleteCollectionProps) {
  if (!db) throw "[deleteCollection] No db provided";
  if (!query && !collectionPath) throw "[deleteCollection] No query OR collectionPath provided";

  let _query: FirebaseFirestore.Query;
  if (query) {
    _query = query;
  } else if (!query && collectionPath) {
    _query = db.collection(collectionPath).orderBy('timestamp');
  }

  return new Promise((resolve, reject) => {
    deleteQueryBatch(db, _query.limit(batchSize), batchSize, resolve, reject);
  });
}

let counter = 0;
function deleteQueryBatch(
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
        console.log(`[deleteQueryBatch] done deleting ${counter}`)
        resolve(counter);
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

export { getCollection }