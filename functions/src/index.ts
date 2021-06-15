import * as functions from "firebase-functions";
import * as admin from "firebase-admin";
import {publishCreatorsWatch} from "./publishCreatorsWatch";
import {analyzeChats} from "./analyzeChats";
import {updateArchives} from "./updateArchives";
import {RuntimeOptions} from "firebase-functions";
import {fetchSuperChats} from "./fetchSuperChats";

admin.initializeApp();

const REGION = "asia-northeast1";
export const WATCH_TOPIC = "watch-creator";
export const ANALYZE_TOPIC = "analyze-chat";
export const TEMP_ANALYZE_TOPIC = "temp-analyze-chat";

const weakRuntimeOpts: RuntimeOptions = {
  timeoutSeconds: 60,
  memory: "128MB",
};
const strongRuntimeOpts: RuntimeOptions = {
  timeoutSeconds: 540,
  memory: "256MB",
};

export const PublishCreatorsWatchFunction = functions.runWith(weakRuntimeOpts).region(REGION).pubsub.schedule("every 30 minutes").onRun(publishCreatorsWatch);
export const UpdateArchivesFunction = functions.runWith(weakRuntimeOpts).region(REGION).pubsub.topic(WATCH_TOPIC).onPublish(updateArchives);
export const AnalyzeChatsFunction = functions.runWith(strongRuntimeOpts).region(REGION).pubsub.topic(ANALYZE_TOPIC).onPublish(analyzeChats);
export const FetchSuperChats = functions.runWith(weakRuntimeOpts).region(REGION).pubsub.schedule("every 5 minutes").onRun(fetchSuperChats);
export const TempAnalyzeChatsFunction = functions.runWith(strongRuntimeOpts).region(REGION).pubsub.topic(TEMP_ANALYZE_TOPIC).onPublish(analyzeChats);

