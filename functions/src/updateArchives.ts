import * as functions from "firebase-functions";
import * as admin from "firebase-admin";
import {Message} from "firebase-functions/lib/providers/pubsub";
import {findArchivedStreams, CHANNEL_ENDPOINT} from "./lib/youtubeArchiveFinder";
import {Bot} from "./lib/discordNotify";

export const updateArchives = async (message: Message) => {
  const channel = messageToJSON(message);
  const bot = new Bot(
      functions.config().discord[channel.category],
      functions.config().discord.system,
      functions.config().discord.activity,
  );
  try {
    const now = new Date();
    const result = await findArchivedStreams(channel.id);
    await saveStream(channel, result.streams);
    if (firstTimeToday(now)) {
      await updateChannel(channel, result.subscribeCount);
    }
  } catch (err) {
    const message = err.message + "\n<" + CHANNEL_ENDPOINT + channel.id + ">\n" + err.stack;
    await bot.alert(message);
    throw new Error(message);
  }
};

const messageToJSON = (message: Message) => {
  const jsonstr = Buffer.from(message.data, "base64").toString("utf-8");
  return JSON.parse(jsonstr);
};

const updateChannel = async (channel: any, subscribeCount: number) => {
  const db = admin.firestore();
  const channelRef = db.collection("channels").doc(channel.id);
  await channelRef.update({
    subscribeCount: subscribeCount,
  }).catch((err) => {
    functions.logger.error(err.message);
  });
};

const saveStream = async (channel: any, streams: Array<any>) => {
  const db = admin.firestore();
  for (const stream of streams) {
    const streamRef = db.collection(`channels/${channel.id}/streams`).doc(stream.id);
    const doc = await streamRef.get().catch((err) => {
      functions.logger.error(err.message);
    });

    if (doc && doc.exists) {
      continue;
    }
    delete stream.id;
    await streamRef.create({...stream,
      chatAvailable: true,
      gameTitle: null,
      chatCount: 0,
      superChatCount: 0,
      superChatAmount: 0,
      subscribeCount: 0,
    }).catch((err) => {
      functions.logger.error(err.message);
    });
  }
};

const firstTimeToday = (now: Date) => {
  // hourはUTCを考慮して0ではなく15にしてる
  return now.getHours() === 15 && (now.getMinutes() >= 0 && now.getMinutes() < 30);
};
