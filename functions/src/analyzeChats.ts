import * as functions from "firebase-functions";
import * as admin from "firebase-admin";
import {findChatMessages, ChatUnavailableError} from "./lib/youtubeChatFinder";
import {Bot} from "./lib/discordNotify";
import {QueryDocumentSnapshot} from "firebase-functions/lib/providers/firestore";
import {EventContext} from "firebase-functions";

export const analyzeChats = async (snapshot: QueryDocumentSnapshot, context: EventContext) => {
  const bot = new Bot(functions.config().discord.general, functions.config().discord.system);
  try {
    const chats = await findChatMessages(snapshot.id, snapshot.get("streamLengthSec"));
    await updateStream(snapshot.id, chats);
    await bot.message(formatMessage(snapshot, chats));
  } catch (err) {
    if (err instanceof ChatUnavailableError) {
      await bot.message(formatMessage(snapshot));
      await bot.alert("チャットがオフになっていいる可能性が高いです\nhttps://www.youtube.com/watch?v=" + snapshot.id);
      functions.logger.warn(snapshot.id + ": チャットがオフになっている可能性が高いです");
    } else {
      await bot.alert(err.message);
      throw err;
    }
  }
};

const updateStream = async (videoId: string, chats: any) => {
  const db = admin.firestore();
  await db.collection("Stream").doc(videoId).update(chats).catch((err) => {
    functions.logger.error(err.message);
  });
};

const formatMessage = (snapshot: QueryDocumentSnapshot, chats?: any) => {
  let message = snapshot.get("title") +
    "\n視聴数: " + threeDigit(snapshot.get("viewCount"));
  if (chats) {
    message += "\nコメント数: " + threeDigit(chats.chatCount) +
    "\nスパチャ数: " + threeDigit(chats.superChatCount) +
    "\nスパチャ額: " + threeDigit(Math.round(chats.superChatAmount)) + "円" +
    "\nメンバー入会数: " + threeDigit(chats.subscribeCount);
  } else {
    message += "\n詳細データの取得に失敗しました(チャットログが非表示の可能性があります)";
  }
  message += "\nhttps://www.youtube.com/watch?v=" + snapshot.id;
  return message;
};

const threeDigit = (num: number) => {
  return String(num).replace( /(\d)(?=(\d\d\d)+(?!\d))/g, "$1,");
};
