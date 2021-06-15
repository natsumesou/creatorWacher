import * as functions from "firebase-functions";
import * as admin from "firebase-admin";
import {findChatMessages, SuperChat} from "./lib/youtubeChatFinder";

export const fetchSuperChats = async () => {
  const db = admin.firestore();
  const channels = await db.collection("channels").where("category", "==", "hololive").get().catch((err) => {
    functions.logger.error(err.message + "\n" + err.stack);
  });

  if (!channels || channels && channels.empty) {
    return;
  }

  const tempChannels: FirebaseFirestore.QueryDocumentSnapshot<FirebaseFirestore.DocumentData>[] = [];

  channels.forEach((channel) => {
    tempChannels.push(channel);
  });

  let count = 0;

  for (const channel of tempChannels) {
    const streams = await channel.ref.collection("streams").get().catch((err) => {
      functions.logger.error(err.message + "\n" + err.stack);
    });

    if (!streams || streams && streams.empty) {
      return;
    }

    const tempStreams: FirebaseFirestore.QueryDocumentSnapshot<FirebaseFirestore.DocumentData>[] = [];
    streams.forEach((stream) => {
      tempStreams.push(stream);
    });

    for (const stream of tempStreams) {
      const superChat = await stream.ref.collection("superChats").limit(1).get();
      if (superChat.size === 0 && stream.get("superChatCount") !== 0) {
        functions.logger.info(`update superchats: ${channel.id}/streams/${stream.id}`);
        const result = await findChatMessages(stream.id, stream.get("streamLengthSec"));
        if (result.stream.chatAvailable) {
          await saveSuperChats(channel.id, stream.id, result.superChats);
        }
        count += 1;
      }
      if (count === 5) {
        break;
      }
    }
    functions.logger.info(`----- updated ${count} streams`);
    if (count === 5) {
      break;
    }
  }
};

const saveSuperChats = async (channelId: string, videoId: string, superChats: {[id:string]: SuperChat}) => {
  const db = admin.firestore();
  let batch = db.batch();
  const limit = 500;
  let i = 0;
  for (const id in superChats) {
    if (!Object.prototype.hasOwnProperty.call(superChats, id)) {
      continue;
    }
    const doc = db.collection(`channels/${channelId}/streams/${videoId}/superChats`).doc(id);
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
