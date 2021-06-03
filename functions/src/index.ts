import * as functions from "firebase-functions";
import * as admin from "firebase-admin";
import {publishCreatorsWatch} from "./publishCreatorsWatch";
import {analyzeChats} from "./analyzeChats";
import {updateArchives} from "./updateArchives";
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
  memory: "256MB",
};

export const PublishCreatorsWatchFunction = functions.runWith(weakRuntimeOpts).region(REGION).pubsub.schedule("every 5 minutes").onRun(publishCreatorsWatch);
export const UpdateArchivesFunction = functions.runWith(weakRuntimeOpts).region(REGION).pubsub.topic(TOPIC).onPublish(updateArchives);
export const AnalyzeChatsFunction = functions.runWith(strongRuntimeOpts).region(REGION).firestore.document("Stream/{videoId}").onCreate(analyzeChats);
