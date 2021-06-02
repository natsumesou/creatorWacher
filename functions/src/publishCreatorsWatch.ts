import * as admin from "firebase-admin";
import {PubSub} from "@google-cloud/pubsub";
import {TOPIC} from "./index";

export const publishCreatorsWatch = async () => {
  const db = admin.firestore();
  const channelRef = db.collection("Channel");
  const channels = await channelRef.get();

  const pubsub = new PubSub({projectId: process.env.GCP_PROJECT});
  const topic = await pubsub.topic(TOPIC);
  channels.forEach((channel) => {
    const obj = {
      id: channel.id,
      title: channel.get("title"),
    };
    topic.publish(Buffer.from(JSON.stringify(obj)));
  });
  return;
};
