import * as functions from "firebase-functions";
import * as admin from "firebase-admin";
import {publishCreatorsWatch} from "./publishCreatorsWatch";
import {WatchCreators} from "./watchCreator";
import {RuntimeOptions} from "firebase-functions";

admin.initializeApp();

const REGION = "asia-northeast1";
export const TOPIC = "watch-creator";

const weakRuntimeOpts: RuntimeOptions = {
  timeoutSeconds: 60,
  memory: "128MB",
};
const strongRuntimeOpts: RuntimeOptions = {
  timeoutSeconds: 540,
  memory: "512MB",
};

export const publishCreatorsWatchFunction = functions.runWith(weakRuntimeOpts).region(REGION).pubsub.schedule("every 5 minutes").onRun(publishCreatorsWatch);
export const WatchCreatorsFunction = functions.runWith(strongRuntimeOpts).region(REGION).pubsub.topic(TOPIC).onPublish(WatchCreators);
