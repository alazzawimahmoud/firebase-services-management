import * as admin from 'firebase-admin';

export type snapshotCallbackType = (snapshot: FirebaseFirestore.QuerySnapshot) => Promise<any>;
export interface IGetCollectionProps {
    db: FirebaseFirestore.Firestore,
    snapshotCallback: snapshotCallbackType
    collectionPath?: string,
    batchSize?: number,
    orderByKey?: string,
    query?: FirebaseFirestore.Query,
}
export default class GetCollection {
    loadCounter: number;
    lastCursor: FirebaseFirestore.QueryDocumentSnapshot | undefined;

    constructor() {
        this.loadCounter = 0;
        this.lastCursor = undefined;
    }

    getCollection({ db, collectionPath, batchSize = 10, orderByKey = 'timestamp', query, snapshotCallback }: IGetCollectionProps) {
        if (!db) throw "[getCollection] No db provided";
        if (!query && !collectionPath) throw "[getCollection] No query OR collectionPath provided";

        let _query: FirebaseFirestore.Query;

        if (query) {
            _query = query;
        } else if (!query && collectionPath) {
            _query = db.collection(collectionPath);
        }

        return new Promise((resolve, reject) => {
            this.getQueryBatch(
                _query.orderBy(orderByKey).limit(batchSize),
                batchSize,
                orderByKey,
                resolve,
                reject,
                snapshotCallback);
        });
    }

    async getQueryBatch(
        query: FirebaseFirestore.Query,
        batchSize: number,
        orderByKey: string,
        resolve: (value?: {} | PromiseLike<{}> | undefined) => void,
        reject: (reason?: any) => void,
        snapshotCallback: snapshotCallbackType
    ) {

        try {

            const snapshot = await query.get()

            // When there are no documents left, we are done
            if (snapshot.size === 0) {
                console.log(`[getQueryBatch] done loading ${this.loadCounter}`)
                resolve(this.loadCounter);
                return;
            }

            // Get the last document
            this.lastCursor = snapshot.docs[snapshot.docs.length - 1];

            // Increment the loadCounter
            this.loadCounter = snapshot.size + this.loadCounter;

            await snapshotCallback(snapshot);

            const nextQuery = query
                .startAfter(this.lastCursor.data()[orderByKey])
                .limit(batchSize);

            console.log(`[getQueryBatch] ${this.loadCounter} loaded`)

            // Recurse on the next process tick, to avoid
            // exploding the stack.
            process.nextTick(() => {
                this.getQueryBatch(nextQuery, batchSize, orderByKey, resolve, reject, snapshotCallback);
            });

        } catch (error) {
            console.error(error);
            reject(error);
        }
    }
}
