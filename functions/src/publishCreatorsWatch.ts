import * as admin from "firebase-admin";
import {PubSub} from "@google-cloud/pubsub";
import {WATCH_TOPIC} from "./index";

export const publishCreatorsWatch = async () => {
  const db = admin.firestore();
  const channelRef = db.collection("channels");
  const channels = await channelRef.get();

  const pubsub = new PubSub({projectId: process.env.GCP_PROJECT});
  const topic = await pubsub.topic(WATCH_TOPIC);
  for (const channel of channels.docs) {
    const obj = {
      id: channel.id,
      category: channel.get("category"),
    };
    await topic.publish(Buffer.from(JSON.stringify(obj)), {});
  }
  return;
};
