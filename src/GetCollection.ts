import * as admin from 'firebase-admin';

type snapshotCallbackType = (snapshot: FirebaseFirestore.QuerySnapshot) => Promise<any>;
interface IGetCollectionProps {
    db: FirebaseFirestore.Firestore,
    snapshotCallback: snapshotCallbackType
    collectionPath?: string,
    batchSize?: number,
    query?: FirebaseFirestore.Query,
}
export default function getCollection({ db, collectionPath, batchSize = 10, query, snapshotCallback }: IGetCollectionProps) {
    if (!db) throw "[getCollection] No db provided";
    if (!query && !collectionPath) throw "[getCollection] No query OR collectionPath provided";

    let _query: FirebaseFirestore.Query;
    if (query) {
        _query = query;
    } else if (!query && collectionPath) {
        _query = db.collection(collectionPath).orderBy('timestamp');
    }

    return new Promise((resolve, reject) => {
        getQueryBatch(_query.limit(batchSize), batchSize, resolve, reject, snapshotCallback);
    });
}

let loadCounter = 0;

function getQueryBatch(
    query: FirebaseFirestore.Query,
    batchSize: number,
    resolve: (value?: {} | PromiseLike<{}> | undefined) => void,
    reject: (reason?: any) => void,
    snapshotCallback: snapshotCallbackType
) {

    console.log(`[getQueryBatch] started `)

    query.get()
        .then((snapshot) => {
            
            console.log(`[getQueryBatch] snapshot.size`, snapshot.size);

            // When there are no documents left, we are done
            if (snapshot.size == 0) {
                return 0;
            }

            return snapshotCallback(snapshot)
                .then(() => {
                    loadCounter = batchSize + loadCounter;
                    return snapshot.size;
                });
        }).then((numLoaded) => {
            if (numLoaded === 0) {
                console.log(`[getQueryBatch] done loading ${loadCounter}`)
                resolve(loadCounter);
                return;
            }
            console.log(`[getQueryBatch] ${loadCounter} loaded`)
            // Recurse on the next process tick, to avoid
            // exploding the stack.
            process.nextTick(() => {
                getQueryBatch(query.limit(batchSize), batchSize, resolve, reject, snapshotCallback);
            });
        })
        .catch(reject);
}