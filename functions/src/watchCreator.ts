import * as functions from "firebase-functions";
import * as admin from "firebase-admin";
import {Message} from "firebase-functions/lib/providers/pubsub";
import {findArchivedStream} from "./lib/youtubeArchiveFinder";
import {findChatMessages, ChatUnavailableError} from "./lib/youtubeChatFinder";
import {Bot} from "./lib/discordNotify";

export const WatchCreators = async (message: Message) => {
  const bot = new Bot(functions.config().discord.token);
  try {
    const channel = messageToJSON(message);
    const stream = await findArchivedStream(channel.id);
    const videoId = stream.id;
    const created = await saveStream(channel, stream);
    if (created) {
      try {
        const chats = await findChatMessages(videoId);
        await updateStream(videoId, chats);
        bot.message(formatMessage(videoId, channel, stream, chats));
      } catch (err) {
        if (err instanceof ChatUnavailableError) {
          bot.message(formatMessage(videoId, channel, stream));
          bot.alert("チャットがオフになっていいる可能性が高いです\nhttps://www.youtube.com/watch?v=" + videoId);
          functions.logger.warn(videoId + ": チャットがオフになっている可能性が高いです");
        } else {
          throw err;
        }
      }
    }
  } catch (err) {
    bot.alert(err.toString());
    throw err;
  }
};

const messageToJSON = (message: Message) => {
  const jsonstr = Buffer.from(message.data, "base64").toString("utf-8");
  return JSON.parse(jsonstr);
};

const saveStream = async (channel: any, stream: any) => {
  const videoId = stream.id;
  const db = admin.firestore();
  const docRef = db.collection("Stream").doc(videoId);
  const doc = await docRef.get();

  if (doc.exists) {
    return false;
  }
  delete stream.id;
  const result = Object.assign(stream, {
    channelId: channel.id,
  });
  await docRef.set(result);
  return true;
};

const updateStream = async (videoId: string, chats: any) => {
  const db = admin.firestore();
  await db.collection("Stream").doc(videoId).update(chats);
};

const formatMessage = (videoId: string, channel: any, stream: any, chats?: any) => {
  let message = stream.title +
    "\nチャンネル: " + channel.title +
    "\n視聴数: " + threeDigit(stream.viewCount);
  if (chats) {
    message += "\nコメント数: " + threeDigit(chats.chatCount) +
    "\nスパチャ数: " + threeDigit(chats.superChatCount) +
    "\nスパチャ額: " + threeDigit(Math.round(chats.superChatAmount)) + "円" +
    "\nメンバー入会数: " + threeDigit(chats.subscribeCount);
  }
  message += "\nhttps://www.youtube.com/watch?v=" + videoId;
  return message;
};

const threeDigit = (num: number) => {
  return String(num).replace( /(\d)(?=(\d\d\d)+(?!\d))/g, "$1,");
};
