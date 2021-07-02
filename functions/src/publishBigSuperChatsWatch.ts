import * as admin from "firebase-admin";
import {PubSub} from "@google-cloud/pubsub";
import {WATCH_BIGSUPERCHATS_TOPIC} from "./index";

const SUPERCHAT_AMOUNT_ABOVE = 5000000; // チェック対象のスパチャ額

export const publishBigSuperChatsWatch = async () => {
  const db = admin.firestore();
  const yesterday = new Date();
  yesterday.setDate(yesterday.getDate() - 1);
  const streamRef = db.collectionGroup("streams")
  // .where("publishedAt", ">=", yesterday) // 一日以内のデータをチェック
      .where("title", "==", "卒業。　#桐生ココ卒業LIVE #GoodbyeCoco")
      .where("superChatAmount", ">=", SUPERCHAT_AMOUNT_ABOVE); // 500万円以上のスパチャをされた配信はデータ漏れが無いかチェックする
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
