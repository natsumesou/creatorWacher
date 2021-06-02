import * as admin from "firebase-admin";
import {Message} from "firebase-functions/lib/providers/pubsub";
import {findArchivedStream} from "./lib/youtubeArchiveFinder";

export const WatchCreators = async (message: Message) => {
  const channel = messageToJSON(message);
  const stream = await findArchivedStream(channel.id);
  await saveStream(channel, stream);
};

const messageToJSON = (message: Message) => {
  const jsonstr = Buffer.from(message.data, "base64").toString("utf-8");
  return JSON.parse(jsonstr);
};

const saveStream = async (channel: any, stream: any) => {
  const videoId = stream.videoId;
  const db = admin.firestore();
  const docRef = db.collection("Stream").doc(videoId);
  const doc = await docRef.get();

  if (!doc.exists) {
    delete stream.videoId;
    const result = Object.assign(stream, {
      channelId: channel.id,
      createdAt: new Date(),
    });
    await docRef.set(result);
  }
};
