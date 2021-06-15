import * as functions from "firebase-functions";
import * as admin from "firebase-admin";
import {findChatMessages, SuperChat} from "./lib/youtubeChatFinder";
import {Message} from "firebase-functions/lib/providers/pubsub";
import {TEMP_ANALYZE_TOPIC} from ".";
import {PubSub} from "@google-cloud/pubsub";
import {Bot} from "./lib/discordNotify";

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

  let counter = 0;

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
        const pubsub = new PubSub({projectId: process.env.GCP_PROJECT});
        const topic = await pubsub.topic(TEMP_ANALYZE_TOPIC);
        const obj = {
          videoId: stream.id,
          channelId: channel.id,
          streamLengthSec: stream.get("streamLengthSec"),
        };
        topic.publish(Buffer.from(JSON.stringify(obj)));
        counter += 1;
      }
      if (counter >= 10) {
        break;
      }
    }
    if (counter >= 10) {
      break;
    }
  }
  if (counter === 0) {
    functions.logger.info("------ all update complated!!");
    const bot = new Bot(
        functions.config().discord.hololive,
        functions.config().discord.system,
        functions.config().discord.activity,
    );
    await bot.alert("hololive superchat completed");
  } else {
    functions.logger.info(`------ published update chats: ${counter}`);
  }
};

export const tempAnalyzeChat = async (message: Message) => {
  const metadata = messageToJSON(message);
  functions.logger.info(`------ tempAnalyzeChat ${metadata.channelId}/streams/${metadata.videoId}`);
  const result = await findChatMessages(metadata.videoId, metadata.streamLengthSec);
  if (result.stream.chatAvailable) {
    await saveSuperChats(metadata.channelId, metadata.videoId, result.superChats);
    functions.logger.info(`------ updated ${metadata.channelId}/streams/${metadata.videoId}`);
  } else {
    functions.logger.info(`------ chat disabled..? ${metadata.channelId}/streams/${metadata.videoId}`);
  }
};

const messageToJSON = (message: Message) => {
  const jsonstr = Buffer.from(message.data, "base64").toString("utf-8");
  const result = JSON.parse(jsonstr);
  result.streamLengthSec = parseInt(result.streamLengthSec);
  return result as {
    channelId: string,
    videoId: string,
    streamLengthSec: number,
  };
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
