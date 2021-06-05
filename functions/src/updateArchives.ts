import * as functions from "firebase-functions";
import * as admin from "firebase-admin";
import {Message} from "firebase-functions/lib/providers/pubsub";
import {findArchivedStreams, CHANNEL_ENDPOINT} from "./lib/youtubeArchiveFinder";
import {Bot} from "./lib/discordNotify";

export const updateArchives = async (message: Message) => {
  const bot = new Bot(
      functions.config().discord.general,
      functions.config().discord.system,
      functions.config().discord.activity,
  );
  const channel = messageToJSON(message);
  try {
    const streams = await findArchivedStreams(channel.id);
    await saveStream(channel, streams);
  } catch (err) {
    const message = err.message + "\n<" + CHANNEL_ENDPOINT + channel.id + ">";
    await bot.alert(message);
    throw new Error(message);
  }
};

const messageToJSON = (message: Message) => {
  const jsonstr = Buffer.from(message.data, "base64").toString("utf-8");
  return JSON.parse(jsonstr);
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
    await streamRef.set({...stream,
      chatUnavailable: false,
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
