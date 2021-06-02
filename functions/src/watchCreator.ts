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
        bot.message(formatMessage(videoId, stream, chats));
      } catch (err) {
        if (err instanceof ChatUnavailableError) {
          bot.message(formatMessage(videoId, stream));
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

const formatMessage = (videoId: string, stream: any, chats?: any) => {
  let message = stream.title +
    "\n視聴数: " + stream.viewCount;
  if (chats) {
    message += "\nコメント数: " + chats.chatCount +
    "\nスパチャ数: " + chats.superChatCount +
    "\nスパチャ額: " + Math.round(chats.superChatAmount).toFixed(2).replace(/\d(?=(\d{3})+\.)/g, "$&,") + "円" +
    "\n入会数: " + chats.subscribeCount;
  }
  message += "\nhttps://www.youtube.com/watch?v=" + videoId;
  return message;
};
