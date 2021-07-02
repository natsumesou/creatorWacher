import * as functions from "firebase-functions";
import {SuperChat} from "./lib/youtubeChatFinder";
import * as admin from "firebase-admin";
import {Message} from "firebase-functions/lib/providers/pubsub";
import {FieldPath} from "@google-cloud/firestore";

export const retriveBigSuperChats = async (message: Message) => {
  const metadata = messageToJSON(message);

  const db = admin.firestore();
  const streamRef = db.collectionGroup("streams").where(FieldPath.documentId(), "==", metadata.id);
  const streams = await streamRef.get().catch((err) => {
    functions.logger.error(err.message + "\n" + err.stack);
  });
  if (!streams) {
    throw new Error(`チャンネルか動画データがfirestoreから取得できません: ${JSON.stringify(metadata)}`);
  }
  const stream = streams.docs[0];
  functions.logger.log("stream: " + stream.id);
  const result = {
    superChats: {
      "bbbbb": {
        supporterChannelId: "b",
        supporterDisplayName: "b test",
        paidAt: new Date(),
        amount: 120,
        unit: "円",
        amountText: "120円",
        thumbnail: "https://example.com",
      },
      "aaaaa": {
        supporterChannelId: "a",
        supporterDisplayName: "a test",
        paidAt: new Date(),
        amount: 1000,
        unit: "円",
        amountText: "1000円",
        thumbnail: "https://example.com",
      },
      "cccc": {
        supporterChannelId: "c",
        supporterDisplayName: "c test",
        paidAt: new Date(),
        amount: 1100,
        unit: "円",
        amountText: "1100円",
        thumbnail: "https://example.com",
      },
    },
  };
  await saveSuperChats(metadata, result.superChats);
};

const messageToJSON = (message: Message) => {
  const jsonstr = Buffer.from(message.data, "base64").toString("utf-8");
  return JSON.parse(jsonstr) as {
    id: string,
    streamLengthSec: string,
  };
};


const saveSuperChats = async (metadata: any, superChats: {[id:string]: SuperChat}) => {
  const db = admin.firestore();
  let batch = db.batch();
  const limit = 500;
  let i = 0;
  for (const id in superChats) {
    if (!Object.prototype.hasOwnProperty.call(superChats, id)) {
      continue;
    }
    const doc = db.collection(`test/${1}/streams/${metadata.videoId}/superChats`).doc(id);
    batch.set(doc, superChats[id]);
    i += 1;
    if (i === limit) {
      await batch.commit().catch((err) => {
        functions.logger.error(err.message + "\n" + err.stack);
      });
      batch = db.batch();
      i = 0;
    }
  }
  if (i !== limit) {
    await batch.commit().catch((err) => {
      functions.logger.error(err.message + "\n" + err.stack);
    });
  }
};
