import * as functions from "firebase-functions";
import * as admin from "firebase-admin";
import {publishCreatorsWatch} from "./publishCreatorsWatch";
import {WatchCreators} from "./watchCreator";

admin.initializeApp();

const REGION = "asia-northeast1";
export const TOPIC = "watch-creator";

export const publishCreatorsWatchFunction = functions.region(REGION).pubsub.schedule("every 5 minutes").onRun(publishCreatorsWatch);
export const WatchCreatorsFunction = functions.region(REGION).pubsub.topic(TOPIC).onPublish(WatchCreators);
