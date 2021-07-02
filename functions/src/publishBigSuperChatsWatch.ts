import * as admin from "firebase-admin";
import {PubSub} from "@google-cloud/pubsub";
import {WATCH_BIGSUPERCHATS_TOPIC} from "./index";

export const publishBigSuperChatsWatch = async () => {
  const db = admin.firestore();
  const yesterday = new Date();
  yesterday.setDate(yesterday.getDate() - 3);
  const streamRef = db.collectionGroup("streams")
      .where("complete", "==", false);
  const streams = await streamRef.get();

  const pubsub = new PubSub({projectId: process.env.GCP_PROJECT});
  const topic = await pubsub.topic(WATCH_BIGSUPERCHATS_TOPIC);
  streams.forEach((stream) => {
    const obj = {
      id: stream.id,
      streamLengthSec: stream.get("streamLengthSec"),
    };
    topic.publish(Buffer.from(JSON.stringify(obj)));
  });
  return;
};
