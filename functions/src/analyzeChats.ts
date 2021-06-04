import * as functions from "firebase-functions";
import * as admin from "firebase-admin";
import {findChatMessages, ChatUnavailableError, ChatNotFoundError} from "./lib/youtubeChatFinder";
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
    } else if (err instanceof ChatNotFoundError) {
      const now = new Date();
      const publishedAt = snapshot.get("publishedAt");
      if (withinAday(now, publishedAt)) {
        // 公開されて1日以内の場合はチャットが戻ってくる可能性があるので一度削除する
        await deleteStream(now, snapshot);

        if (withinOneHour(now, publishedAt)) {
          const message = "チャットがオフになっている(もしくはYouTubeの仕様が変わった)可能性が高いです\nhttps://www.youtube.com/watch?v=" + snapshot.id;
          await bot.alert(message);
          functions.logger.warn(message);
        }
      } else {
        const message = "チャットが戻らないまま1日以上経ったので追跡を終了します\nhttps://www.youtube.com/watch?v=" + snapshot.id;
        await bot.alert(message);
      }
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

const deleteStream = async (now: Date, snapshot: QueryDocumentSnapshot) => {
  const db = admin.firestore();
  await db.collection("Stream").doc(snapshot.id).delete().catch((err) => {
    functions.logger.error(err.message);
  });
};

const withinOneHour = (now: Date, publishedAt: Date) => {
  const millisecondsPerDay = 1000 * 60;
  const millisBetween = now.getTime() - publishedAt.getTime();
  const minutes = millisBetween / millisecondsPerDay;
  return minutes < 60;
};

const withinAday = (now: Date, publishedAt: Date) => {
  const millisecondsPerDay = 1000 * 60 * 60 * 24;
  const millisBetween = now.getTime() - publishedAt.getTime();
  const days = millisBetween / millisecondsPerDay;
  return days < 1;
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
    message += "\nチャットがオフのため詳細データなし";
  }
  message += "\nhttps://www.youtube.com/watch?v=" + snapshot.id;
  return message;
};

const threeDigit = (num: number) => {
  return String(num).replace( /(\d)(?=(\d\d\d)+(?!\d))/g, "$1,");
};
