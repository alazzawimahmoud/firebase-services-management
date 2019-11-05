import * as admin from 'firebase-admin';

type snapshotCallbackType = (snapshot: FirebaseFirestore.QuerySnapshot) => Promise<any>;
interface IGetCollectionProps {
    db: FirebaseFirestore.Firestore,
    snapshotCallback: snapshotCallbackType
    collectionPath?: string,
    batchSize?: number,
    orderByKey?: string,
    query?: FirebaseFirestore.Query,
}

export default function getCollection({ db, collectionPath, batchSize = 10, orderByKey = 'timestamp', query, snapshotCallback }: IGetCollectionProps) {
    if (!db) throw "[getCollection] No db provided";
    if (!query && !collectionPath) throw "[getCollection] No query OR collectionPath provided";

    let _query: FirebaseFirestore.Query;

    if (query) {
        _query = query;
    } else if (!query && collectionPath) {
        _query = db.collection(collectionPath);
    }

    return new Promise((resolve, reject) => {
        getQueryBatch(
            db,
            _query.orderBy(orderByKey).limit(batchSize),
            batchSize,
            orderByKey,
            resolve,
            reject,
            snapshotCallback);
    });
}

let loadCounter = 0;
let lastCursor: FirebaseFirestore.QueryDocumentSnapshot;

function getQueryBatch(
    db: FirebaseFirestore.Firestore,
    query: FirebaseFirestore.Query,
    batchSize: number,
    orderByKey: string,
    resolve: (value?: {} | PromiseLike<{}> | undefined) => void,
    reject: (reason?: any) => void,
    snapshotCallback: snapshotCallbackType
) {

    console.log(`[getQueryBatch] started `)



    query.get()
        .then((snapshot) => {

            console.log(`[getQueryBatch] snapshot.size`, snapshot.size);

            // When there are no documents left, we are done
            if (snapshot.size === 0) {
                return 0;
            }

            // Get the last document
            lastCursor = snapshot.docs[snapshot.docs.length - 1];

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

            // Construct a new query starting at this document.
            // Note: this will not have the desired effect if multiple
            // cities have the exact same population value.
            let nextQuery = query
                .orderBy(orderByKey)
                .startAfter(lastCursor.data()[orderByKey])
                .limit(batchSize);

            console.log(`[getQueryBatch] ${loadCounter} loaded`)
            // Recurse on the next process tick, to avoid
            // exploding the stack.
            process.nextTick(() => {
                getQueryBatch(db, nextQuery, batchSize, orderByKey, resolve, reject, snapshotCallback);
            });
        })
        .catch(reject);
}