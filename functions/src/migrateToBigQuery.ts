import * as functions from "firebase-functions";
import * as admin from "firebase-admin";
import {ChangeType, migrateStreamsToBigQuery, migrateSuperChatsToBigQuery} from "./exportToBigQuery";
import {DocumentSnapshot} from "firebase-functions/lib/providers/firestore";
import {Message} from "firebase-functions/lib/providers/pubsub";
import {PubSub} from "@google-cloud/pubsub";
import {TEMP_ANALYZE_TOPIC} from ".";

export const migrateStreams = async () => {
  const db = admin.firestore();
  const channels = await db.collection("channels").get().catch((err) => {
    functions.logger.error(err.message + "\n" + err.stack);
  });

  if (!channels || channels && channels.empty) {
    return;
  }

  const tempChannels: FirebaseFirestore.QueryDocumentSnapshot<FirebaseFirestore.DocumentData>[] = [];

  channels.forEach((channel) => {
    tempChannels.push(channel);
  });

  for await (const channel of tempChannels) {
    const streams = await channel.ref.collection("streams").get().catch((err) => {
      functions.logger.error(err.message + "\n" + err.stack);
    });

    if (!streams || streams && streams.empty) {
      continue;
    }

    const tempStreams: DocumentSnapshot[] = [];
    streams.forEach((stream) => {
      tempStreams.push(stream);
    });

    functions.logger.info("migrate channel " + channel.id + " videos: " + tempStreams.length);
    await migrateStreamsToBigQuery(channel, tempStreams, ChangeType.CREATE);
  }
};

export const triggerSuperChats = async () => {
  const db = admin.firestore();
  const channels = await db.collection("channels").where("videoId", "==", "S1Kx7Hc018c").get().catch((err) => {
    functions.logger.error(err.message + "\n" + err.stack);
  });

  if (!channels || channels && channels.empty) {
    return;
  }

  const tempChannels: FirebaseFirestore.QueryDocumentSnapshot<FirebaseFirestore.DocumentData>[] = [];

  channels.forEach((channel) => {
    tempChannels.push(channel);
  });

  const pubsub = new PubSub({projectId: process.env.GCP_PROJECT});
  const topic = await pubsub.topic(TEMP_ANALYZE_TOPIC);

  for await (const channel of tempChannels) {
    const streams = await channel.ref.collection("streams").get().catch((err) => {
      functions.logger.error(err.message + "\n" + err.stack);
    });
    if (!streams || streams && streams.empty) {
      continue;
    }
    let count = 0;
    const tempStreams: FirebaseFirestore.QueryDocumentSnapshot<FirebaseFirestore.DocumentData>[] = [];
    streams.forEach((stream) => tempStreams.push(stream));
    for await (const stream of tempStreams) {
      if (count < 1) {
        await topic.publish(Buffer.from(JSON.stringify({channelId: channel.id, videoId: stream.id})));
        count += 1;
      }
    }
    break;
  }
};

export const migrateSuperChats = async (message: Message) => {
  const params = messageToJSON(message);
  const channelId = params.channelId;
  const videoId = params.videoId;
  const db = admin.firestore();
  const superChats = await db.collection(`channels/${channelId}/streams/${videoId}/superChats`).get().catch((err) => {
    functions.logger.error(err.message + "\n" + err.stack);
  });

  if (!superChats || superChats && superChats.empty) {
    return;
  }

  const tempSuperChats: DocumentSnapshot[] = [];
  superChats.forEach((sc) => {
    tempSuperChats.push(sc);
  });
  functions.logger.info("migrate video /" + channelId + "/streams/" + videoId + " sc: " + superChats.size);
  await migrateSuperChatsToBigQuery(tempSuperChats, channelId, videoId, ChangeType.CREATE);
};

const messageToJSON = (message: Message) => {
  const jsonstr = Buffer.from(message.data, "base64").toString("utf-8");
  return JSON.parse(jsonstr);
};
