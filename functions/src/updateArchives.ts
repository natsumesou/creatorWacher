import * as functions from "firebase-functions";
import * as admin from "firebase-admin";
import {Message} from "firebase-functions/lib/providers/pubsub";
import {findArchivedStreams} from "./lib/youtubeArchiveFinder";

export const updateArchives = async (message: Message) => {
  const channel = messageToJSON(message);
  const streams = await findArchivedStreams(channel.id);
  await saveStream(channel, streams);
};

const messageToJSON = (message: Message) => {
  const jsonstr = Buffer.from(message.data, "base64").toString("utf-8");
  return JSON.parse(jsonstr);
};

const saveStream = async (channel: any, streams: Array<any>) => {
  const db = admin.firestore();
  for (const stream of streams) {
    const docRef = db.collection("Stream").doc(stream.id);
    const doc = await docRef.get().catch((err) => {
      functions.logger.error(err.message);
    });

    if (doc && doc.exists) {
      continue;
    }
    delete stream.id;
    const result = Object.assign(stream, {
      channelId: channel.id,
    });
    await docRef.set(result).catch((err) => {
      functions.logger.error(err.message);
    });
  }
};
